"""
compute_analysis.py
全限月対応版 + MaxPain ATM±30%範囲計算 + PCR + GammaFlip改善
2026年2月24日以降の新列構造対応（横持ち形式）
"""

import json
import math
import logging
import pandas as pd
from pathlib import Path
from datetime import date
from calendar import monthrange

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
DATA_DIR = Path("data")

# ── ブラック・ショールズ ──────────────────────────────

def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def bs_greeks(S, K, T, sigma, opt_type, r=0.005):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {}
    sq = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sq)
    d2 = d1 - sigma * sq
    pdf1 = norm_pdf(d1)
    gamma = pdf1 / (S * sigma * sq)
    vega  = S * pdf1 * sq / 100
    if opt_type == "C":
        price = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
        delta = norm_cdf(d1)
        theta = (-S * pdf1 * sigma / (2 * sq) - r * K * math.exp(-r * T) * norm_cdf(d2)) / 252
        rho   = K * T * math.exp(-r * T) * norm_cdf(d2) / 100
    else:
        price = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
        delta = norm_cdf(d1) - 1
        theta = (-S * pdf1 * sigma / (2 * sq) + r * K * math.exp(-r * T) * norm_cdf(-d2)) / 252
        rho   = -K * T * math.exp(-r * T) * norm_cdf(-d2) / 100
    return {
        "price": round(price, 2), "delta": round(delta, 4),
        "gamma": round(gamma, 8), "theta": round(theta, 2),
        "vega":  round(vega, 4),  "rho":   round(rho, 4),
    }


# ── IV逆算（二分法）────────────────────────────────
def _bs_price_only(S, K, T, sigma, r, opt_type):
    """BS価格のみ計算（高速化）"""
    if T <= 0 or sigma <= 0:
        return max(S-K,0) if opt_type=="C" else max(K-S,0)
    sq = math.sqrt(T)
    d1 = (math.log(S/K)+(r+0.5*sigma**2)*T)/(sigma*sq)
    d2 = d1-sigma*sq
    if opt_type=="C":
        return S*norm_cdf(d1)-K*math.exp(-r*T)*norm_cdf(d2)
    return K*math.exp(-r*T)*norm_cdf(-d2)-S*norm_cdf(-d1)

def _bisect_iv(S, K, T, market_price, opt_type, r, tol=0.01, max_iter=200):
    """指定opt_typeでIVを二分法逆算。失敗時None。"""
    intrinsic = max(S-K,0) if opt_type=="C" else max(K-S,0)
    if market_price <= intrinsic + 0.001:
        return None
    lo, hi = 0.001, 20.0
    for _ in range(max_iter):
        mid = (lo+hi)/2
        p = _bs_price_only(S,K,T,mid,r,opt_type)
        if abs(p-market_price) < tol:
            return mid if 0.005 < mid < 5.0 else None
        if p < market_price: lo = mid
        else: hi = mid
    result = (lo+hi)/2
    return result if 0.005 < result < 5.0 else None

def implied_vol(S, K, T, market_price, opt_type="C", r=0.005):
    """
    TheoreticalPrice から IV を逆算する。
    JPX CSV の列構造:
      - CallClose       = コール終値
      - TheoreticalPrice = プット理論価格  ← 注意！コールではない
    そのため opt_type="P" で逆算するのが正しい。
    コール・プット両方試して有効な方を返す。
    """
    if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return None
    # まず指定opt_typeで試す
    iv = _bisect_iv(S, K, T, market_price, opt_type, r)
    if iv is not None:
        return iv
    # 失敗したら逆のopt_typeで試す
    other = "P" if opt_type == "C" else "C"
    return _bisect_iv(S, K, T, market_price, other, r)

# ── 満期日（第2金曜日）─────────────────────────────

def expiry_date(contract_month_dt):
    y, m = int(contract_month_dt.year), int(contract_month_dt.month)
    days = monthrange(y, m)[1]
    fridays = [date(y, m, d) for d in range(1, days + 1) if date(y, m, d).weekday() == 4]
    return fridays[1] if len(fridays) >= 2 else fridays[0]

# ── MaxPain（ATM±30%の行使価格のみ対象）──────────────

def calc_max_pain(df, strikes, S, T, r=0.005):
    """
    MaxPain = オプション売り手の損失が最小になる行使価格。

    【OI代用方針】実OI（fetch_oi.py）がない場合:
    CallClose（コール終値）をOI代用として使用する。
    - K > S (OTMコール): CallClose = コール終値（小さい → OTMほど小さい）
    - K < S (OTMプット): CallClose = プット終値（小さい → OTMほど小さい）
    これは限月・行使価格ごとに異なる実際の市場価格を反映するため
    限月間で異なるMaxPainが計算される。

    実OI（fetch_oi.py）が取得できた場合はそちらが優先される。
    """
    lower = S * 0.85
    upper = S * 1.15
    target_strikes = [k for k in strikes if lower <= k <= upper]
    if not target_strikes:
        target_strikes = [k for k in strikes if S*0.70 <= k <= S*1.30]
    if not target_strikes:
        target_strikes = strikes

    # CallCloseを行使価格→終値のマップとして取得
    cc_map = {}
    for _, row in df.iterrows():
        k = row.get("StrikePrice")
        cc = row.get("CallClose")
        if k is not None and not pd.isna(k) and cc is not None and not pd.isna(cc):
            cc_val = float(cc)
            if cc_val > 0:
                cc_map[float(k)] = cc_val

    if not cc_map:
        atm_strike = min(target_strikes, key=lambda k: abs(k-S))
        log.warning("MaxPain: CallCloseデータなし → ATM返却")
        return atm_strike

    # CallCloseをOI代用としてMaxPain計算
    # コール(K>=S): pain = CallClose(K) × max(0, k_exp - K)  （コール売り手の損失）
    # プット(K<S):  pain = CallClose(K) × max(0, K - k_exp)  （プット売り手の損失）
    pain = {}
    for k_exp in target_strikes:
        total = 0.0
        for k in target_strikes:
            oi = cc_map.get(k, 0.0)
            if k >= S:
                total += oi * max(0.0, k_exp - k)
            else:
                total += oi * max(0.0, k - k_exp)
        pain[k_exp] = total

    # 実OIなしでのMaxPain計算結果はほぼATM付近になる
    # 全限月で同じ値になることを避けるため、実OIなし時は None を返す
    # → UIで「建玉取得後に更新」と表示
    # ※ oi_data がある場合はこの関数は呼ばれない（analyze_month内で分岐）
    mp = min(pain, key=pain.get) if pain else None
    log.info("MaxPain計算(CallClose代用): ATM=%.0f 対象=%d 結果=%s", S, len(target_strikes), mp)
    return mp

# ── PCR（Put/Call Ratio）─────────────────────────────

def calc_pcr(df, S):
    """
    PCR = PUT終値合計 / CALL終値合計（ATM±30%の行使価格のみ）
    """
    lower, upper = S * 0.70, S * 1.30
    df_f = df[(df["StrikePrice"] >= lower) & (df["StrikePrice"] <= upper)].copy()
    call_sum = pd.to_numeric(df_f["CallClose"], errors="coerce").fillna(0).sum()
    # PutCloseは新形式では直接ないため TheoreticalPrice の代用
    # コール終値合計 vs 全体の対称性からPCRを推定
    # → call_sum が有効な場合、strike分布の非対称性からPCRを算出
    # 実際のOIがないため、コール終値の逆数的推定
    # 有効な近似: PCR = PUT偏重OI推定 / CALL偏重OI推定
    # ATM以下の終値合計(プット寄り) / ATM以上の終値合計(コール寄り)
    atm = min(df_f["StrikePrice"].dropna().tolist(), key=lambda k: abs(k - S)) if not df_f.empty else S
    call_side = pd.to_numeric(df_f[df_f["StrikePrice"] >= atm]["CallClose"], errors="coerce").fillna(0).sum()
    put_side  = pd.to_numeric(df_f[df_f["StrikePrice"] <  atm]["CallClose"], errors="coerce").fillna(0).sum()
    if call_side > 0:
        pcr = round(put_side / call_side, 3)
    else:
        pcr = None
    log.info("PCR推定: call_side=%.0f put_side=%.0f PCR=%s", call_side, put_side, pcr)
    return pcr

# ── GEX・GammaFlip ──────────────────────────────────

def calc_gex(df, S, T, r=0.005):
    """
    GEX計算（時間価値OI代用版）。

    OIの代用として「時間価値」を使用:
    - K >= S (OTMコール): OI = CallClose（本質的価値=0なので時間価値=CallClose）
    - K <  S (OTMプット): OI = CallClose - (S - K) = 時間価値
      ※ K<SのCallCloseはITMコール終値 = 本質的価値(S-K) + 時間価値
      ※ 時間価値のみを使うことでATM付近が最大のOI分布になる

    GEX符号:
    - K >= S (コール): +GEX（ディーラーロングガンマ→安定化）
    - K <  S (プット): -GEX（ディーラーショートガンマ→不安定化）
    """
    gex_list = []
    for _, row in df.iterrows():
        K  = row["StrikePrice"]
        cc = row.get("CallClose")
        if pd.isna(K) or pd.isna(cc) or float(cc) <= 0:
            continue
        K_f = float(K)
        cc_f = float(cc)

        # 時間価値をOI代用として計算
        intrinsic = max(S - K_f, 0)   # コールの本質的価値
        time_value = max(0.0, cc_f - intrinsic)
        if time_value <= 0:
            continue

        # IV逆算: OTMコール=コール価格、OTMプット=プット価格（put-call parity）
        opt_type = "C" if K_f >= S else "P"
        # プット価格 ≈ 時間価値（ATM付近で成立）
        price_for_iv = time_value if K_f < S else cc_f
        iv_dec = implied_vol(S, K_f, T, price_for_iv, opt_type, r)
        if iv_dec is None:
            # フォールバック: ATM IVを使用
            iv_dec = 0.23
        g = bs_greeks(S, K_f, T, iv_dec, "C", r)
        if not g:
            continue
        gamma = g["gamma"]
        sign = 1.0 if K_f >= S else -1.0
        gex_list.append({
            "StrikePrice": K_f,
            "GEX": round(sign * gamma * time_value * S * S / 100, 2)
        })
    if not gex_list:
        return pd.DataFrame(columns=["StrikePrice", "GEX"])
    return pd.DataFrame(gex_list).groupby("StrikePrice")["GEX"].sum().reset_index()

def find_gamma_flip(gex_df, S):
    """
    先物価格に最も近いGEXの正→負転換点を返す。
    先物より上側と下側の両方を探し、より近い方を採用。
    """
    if gex_df.empty:
        return None

    flip = None
    best_dist = float("inf")

    gex_sorted = gex_df.sort_values("StrikePrice").reset_index(drop=True)

    for i in range(len(gex_sorted) - 1):
        g1 = gex_sorted.iloc[i]["GEX"]
        g2 = gex_sorted.iloc[i + 1]["GEX"]
        k1 = gex_sorted.iloc[i]["StrikePrice"]
        k2 = gex_sorted.iloc[i + 1]["StrikePrice"]
        if g1 >= 0 > g2 or g1 < 0 <= g2:
            # 転換点の中点を使用
            mid = (k1 + k2) / 2
            dist = abs(mid - S)
            if dist < best_dist:
                best_dist = dist
                flip = round((k1 + k2) / 2)

    return flip

# ── ATM IV取得（有効値のみ）─────────────────────────

def get_atm_iv(df, S, T, r=0.005):
    """
    ATM付近のIVを取得。
    JPX CSV の IV列・TheoreticalPrice列は全行ダミー値のため使用不可。
    CallClose列（コール終値）を使い、OTMコール（K > S）からIVを逆算する。
    ATMに最も近いOTMコールから順に試みる。
    """
    # OTMコール（K > S）のみを対象（K <= S はITMでIV逆算不安定）
    df_otm = df[df["StrikePrice"] > S].copy()
    if df_otm.empty:
        log.warning("OTMコールが存在しない → デフォルト35%%使用")
        return 0.35

    # ATMに近い順（K昇順 = 最もATMに近いOTMから）
    df_otm = df_otm.sort_values("StrikePrice")

    for _, row in df_otm.iterrows():
        K = row.get("StrikePrice")
        cc = row.get("CallClose")
        if pd.isna(K) or pd.isna(cc) or float(cc) <= 0:
            continue
        iv = implied_vol(S, K, T, float(cc), "C", r)
        if iv is not None:
            log.info("IV逆算成功(OTMコール): K=%.0f CallClose=%.2f IV=%.4f (%.1f%%)",
                     K, cc, iv, iv*100)
            return iv

    log.warning("IV逆算失敗 → デフォルト35%%使用")
    return 0.35

# ── 1限月分の分析 ──────────────────────────────────

def analyze_month(df_m, S, month_str, today, oi_data=None):
    """1限月分のデータを分析してdictを返す"""
    cmonth_dt = df_m["ContractMonthDt"].dropna()
    if cmonth_dt.empty:
        return None
    cmonth_dt = cmonth_dt.iloc[0]

    exp  = expiry_date(cmonth_dt)
    dte  = max((exp - today).days, 0)
    T    = max(dte, 1) / 252.0

    strikes = sorted(df_m["StrikePrice"].dropna().unique().tolist())
    if not strikes:
        return None

    # ATM
    atm_strike = min(strikes, key=lambda k: abs(k - S))
    atm_diff   = round(atm_strike - S)

    # ATM IV
    atm_iv_raw = get_atm_iv(df_m, S, T)
    atm_iv_dec = atm_iv_raw if atm_iv_raw < 1 else atm_iv_raw / 100

    # グリークス
    call_g = bs_greeks(S, atm_strike, T, atm_iv_dec, "C")
    put_g  = bs_greeks(S, atm_strike, T, atm_iv_dec, "P")

    # MaxPain（実OIがあれば使用、なければ終値代用）
    if oi_data:
        # 実OI使用
        lower, upper = S * 0.70, S * 1.30
        oi_filtered = {k: v for k, v in oi_data.items() if lower <= k <= upper}
        if oi_filtered:
            pain = {}
            for k_exp in oi_filtered:
                total = sum(v["call_oi"] * max(0, k - k_exp) + v["put_oi"] * max(0, k_exp - k)
                           for k, v in oi_filtered.items())
                pain[k_exp] = total
            max_pain = min(pain, key=pain.get) if pain else S
            log.info("MaxPain(実OI): %.0f", max_pain)
        else:
            max_pain = calc_max_pain(df_m, strikes, S, T)
    else:
        max_pain = calc_max_pain(df_m, strikes, S, T)

    # PCR（実OIがあれば使用）
    if oi_data:
        lower, upper = S * 0.70, S * 1.30
        oi_f = {k: v for k, v in oi_data.items() if lower <= k <= upper}
        if oi_f:
            call_sum = sum(v["call_oi"] for v in oi_f.values())
            put_sum  = sum(v["put_oi"]  for v in oi_f.values())
            pcr = round(put_sum / call_sum, 3) if call_sum > 0 else None
            log.info("PCR(実OI): %s (CALL=%.0f PUT=%.0f)", pcr, call_sum, put_sum)
        else:
            pcr = calc_pcr(df_m, S)
    else:
        pcr = calc_pcr(df_m, S)

    # GEX
    gex_df = calc_gex(df_m, S, T)
    total_gex = round(gex_df["GEX"].sum() / 1_000_000, 2) if not gex_df.empty else 0.0
    gex_pos   = total_gex >= 0

    # GammaFlip
    gamma_flip = find_gamma_flip(gex_df, S)

    # Call Wall / Put Wall（GEX最大・最小の行使価格）
    if not gex_df.empty:
        call_wall = float(gex_df.loc[gex_df["GEX"].idxmax(), "StrikePrice"])
        put_wall  = float(gex_df.loc[gex_df["GEX"].idxmin(), "StrikePrice"]) if (gex_df["GEX"] < 0).any() else None
    else:
        call_wall = put_wall = None

    # CALL IV / PUT IV（ATM付近）
    iv_near = df_m[(df_m["IV"].notna()) & (df_m["IV"] > 0.05)].copy()
    iv_near["dist"] = (iv_near["StrikePrice"] - S).abs()
    iv_near_sorted = iv_near.sort_values("dist")

    call_iv = round(atm_iv_dec * 100, 2)  # ATM IVをコールIVとして使用
    # プットIVはATMより少し低い行使価格のIV
    put_side = iv_near_sorted[iv_near_sorted["StrikePrice"] < atm_strike]
    put_iv   = round(float(put_side.iloc[0]["IV"]) * 100, 2) if not put_side.empty else call_iv
    if put_iv > 1:  # パーセント表記チェック
        put_iv = round(put_iv / 100, 2)

    rr = round(call_iv - put_iv, 2)

    # Implied Move（ATM IV × √(DTE/365) × 先物）
    im     = round(S * atm_iv_dec * (dte / 365) ** 0.5) if dte > 0 else None
    im_pct = round(im / S * 100, 1) if im else None

    # IV・価格サマリー（ATM±30%）
    lower, upper = S * 0.70, S * 1.30
    strikes_summary = []
    for k in strikes:
        if not (lower <= k <= upper):
            continue
        row = df_m[df_m["StrikePrice"] == k]
        if row.empty:
            continue
        r = row.iloc[0]
        # IV列・TheoreticalPrice列はダミー → CallCloseからIV逆算
        cc_v = r.get("CallClose")
        cc_v = float(cc_v) if not pd.isna(cc_v) else 0.0
        if cc_v > 0:
            opt_type_v = "C" if k > S else "P"
            iv_dec_v = implied_vol(S, k, T, cc_v, opt_type_v)
            iv_v = round(iv_dec_v * 100, 2) if iv_dec_v else None
        else:
            iv_v = None
        strikes_summary.append({
            "strike":      k,
            "iv":          iv_v,
            "call_close":  float(r["CallClose"])      if not pd.isna(r.get("CallClose", float("nan")))      else None,
            "theoretical": float(r["TheoreticalPrice"]) if not pd.isna(r.get("TheoreticalPrice", float("nan"))) else None,
            "delta":       float(r["Delta"])           if "Delta" in r and not pd.isna(r.get("Delta", float("nan"))) else None,
        })

    return {
        "month":           month_str,
        "expiry_date":     exp.isoformat(),
        "dte":             dte,
        "atm_strike":      atm_strike,
        "atm_diff":        atm_diff,
        "atm_iv_pct":      call_iv,
        "call_iv":         call_iv,
        "put_iv":          put_iv,
        "rr":              rr,
        "max_pain":        max_pain,
        "mp_diff":         round(max_pain - S) if max_pain is not None else None,
        "gamma_flip":      gamma_flip,
        "total_gex_m":     total_gex,
        "gex_pos":         bool(gex_pos),
        "call_wall":       call_wall,
        "put_wall":        put_wall,
        "pcr":             pcr,
        "im":              im,
        "im_pct":          im_pct,
        "im_upper":        round(atm_strike + im) if im else None,
        "im_lower":        round(atm_strike - im) if im else None,
        "atm_call_greeks": call_g,
        "atm_put_greeks":  put_g,
        "gex_by_strike":   gex_df.to_dict(orient="records"),
        "strikes_summary": strikes_summary,
    }

# ── メイン ────────────────────────────────────────

def main():
    csv_path = DATA_DIR / "options_latest.csv"
    if not csv_path.exists():
        log.error("options_latest.csv が見つかりません")
        raise SystemExit(1)

    df = pd.read_csv(csv_path, dtype={"ContractMonth": str})
    df["ContractMonth"]   = df["ContractMonth"].str.strip()
    df["ContractMonthDt"] = pd.to_datetime(df["ContractMonth"], format="%Y%m", errors="coerce")

    # 原資産価格（日経225現物終値）
    # UnderlyingClose は行使価格連動の擬似値のため使用禁止
    # BaseVolatility が全行共通の正しい現物終値
    bv = df["BaseVolatility"].dropna() if "BaseVolatility" in df.columns else pd.Series(dtype=float)
    if not bv.empty:
        S = float(bv.iloc[0])
    else:
        meta_path = DATA_DIR / "meta.json"
        S = json.loads(meta_path.read_text()).get("underlying_close", 38000.0) if meta_path.exists() else 38000.0
    log.info("原資産価格（日経225現物終値）: %.2f", S)

    today = date.today()

    # 全限月を取得してソート（YYYYMM形式=6桁の月次限月のみ対象、週次限月は除外）
    all_months = df["ContractMonth"].dropna().unique().tolist()
    months = sorted([m for m in all_months if len(str(m).strip()) == 6])
    log.info("月次限月のみ処理: %s", months)

    # OIデータ（fetch_oi.pyが生成）を読み込む
    oi_path = DATA_DIR / "oi_latest.json"
    oi_by_month = {}
    if oi_path.exists():
        try:
            oi_json = json.loads(oi_path.read_text())
            for month, data in oi_json.get("months", {}).items():
                oi_by_month[month] = {
                    float(k): v for k, v in data.get("oi_by_strike", {}).items()
                }
            log.info("OIデータ読込成功: %d限月", len(oi_by_month))
        except Exception as e:
            log.warning("OIデータ読込失敗: %s → 終値代用", e)
    else:
        log.info("oi_latest.json なし → 終値代用でMaxPain・PCR計算")

    # 限月別分析
    month_results = []
    for month_str in months:
        df_m = df[df["ContractMonth"] == month_str].copy()
        log.info("分析中: %s (%d行)", month_str, len(df_m))
        result = analyze_month(df_m, S, month_str, today, oi_data=oi_by_month.get(month_str))
        if result:
            month_results.append(result)

    if not month_results:
        log.error("分析結果なし")
        raise SystemExit(1)

    # 直近限月
    nearest = month_results[0]

    # 全体出力
    out = {
        "generated_at":    today.isoformat(),
        "underlying":      {"name": "日経225", "close": S},
        # 後方互換性のため直近限月の値もトップレベルに残す
        "nearest_month":   nearest["month"],
        "expiry_date":     nearest["expiry_date"],
        "dte":             nearest["dte"],
        "max_pain":        nearest["max_pain"],
        "gamma_flip":      nearest["gamma_flip"],
        "atm_strike":      nearest["atm_strike"],
        "atm_iv_pct":      nearest["atm_iv_pct"],
        "atm_call_greeks": nearest["atm_call_greeks"],
        "atm_put_greeks":  nearest["atm_put_greeks"],
        "gex_by_strike":   nearest["gex_by_strike"],
        "strikes_summary": nearest["strikes_summary"],
        # 全限月データ
        "months":          month_results,
    }

    out_path = DATA_DIR / "analysis_latest.json"
    class SafeEncoder(json.JSONEncoder):
        def default(self, obj):
            import numpy as np
            if isinstance(obj, (np.bool_, bool)):
                return bool(obj)
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            return super().default(obj)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2, cls=SafeEncoder))
    log.info("保存完了: %s", out_path)

    # 限月別サマリーCSVも出力
    summary_rows = []
    for m in month_results:
        summary_rows.append({
            "month": m["month"], "dte": m["dte"],
            "atm": m["atm_strike"], "max_pain": m["max_pain"], "mp_diff": m["mp_diff"],
            "gamma_flip": m["gamma_flip"], "total_gex_m": m["total_gex_m"], "gex_pos": m["gex_pos"],
            "call_iv": m["call_iv"], "put_iv": m["put_iv"], "rr": m["rr"],
            "pcr": m["pcr"], "im": m["im"], "im_pct": m["im_pct"],
        })
    pd.DataFrame(summary_rows).to_csv(DATA_DIR / "summary_latest.csv", index=False, encoding="utf-8-sig")
    log.info("サマリーCSV保存完了")

if __name__ == "__main__":
    main()
