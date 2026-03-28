"""
compute_analysis.py
options_latest.csv を読み込み、グリークス・マックスペイン・ガンマエクスポージャーを計算して
data/analysis_latest.json に出力する。
"""

import json
import math
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path("data")


# ─────────────────────────────────────────────
# ブラック・ショールズ計算
# ─────────────────────────────────────────────

def norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def _d1d2(S, K, T, sigma, r=0.005):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return None, None
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return d1, d2

def bs_greeks(S: float, K: float, T: float, sigma: float, opt_type: str, r: float = 0.005) -> dict:
    """Delta, Gamma, Theta, Vega, Rho を返す"""
    d1, d2 = _d1d2(S, K, T, sigma, r)
    if d1 is None:
        return {}
    sqrt_T = math.sqrt(T)
    pdf_d1 = norm_pdf(d1)
    gamma = pdf_d1 / (S * sigma * sqrt_T)
    vega  = S * pdf_d1 * sqrt_T / 100          # IV 1% 変化あたり

    if opt_type == "C":
        delta = norm_cdf(d1)
        theta = (-S * pdf_d1 * sigma / (2 * sqrt_T)
                 - r * K * math.exp(-r * T) * norm_cdf(d2)) / 252
        rho   = K * T * math.exp(-r * T) * norm_cdf(d2) / 100
        price = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        delta = norm_cdf(d1) - 1
        theta = (-S * pdf_d1 * sigma / (2 * sqrt_T)
                 + r * K * math.exp(-r * T) * norm_cdf(-d2)) / 252
        rho   = -K * T * math.exp(-r * T) * norm_cdf(-d2) / 100
        price = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

    return {
        "price": round(price, 2),
        "delta": round(delta, 4),
        "gamma": round(gamma, 8),
        "theta": round(theta, 2),
        "vega":  round(vega, 4),
        "rho":   round(rho, 4),
    }


# ─────────────────────────────────────────────
# マックスペイン計算
# ─────────────────────────────────────────────

def calc_max_pain(df_call: pd.DataFrame, df_put: pd.DataFrame, strikes: list) -> float:
    """
    各行使価格について全コール・プットOI×（行使価格との差）の合計（痛みの総和）を計算し、
    最小となる行使価格をマックスペインとして返す。
    OI の代理として TheoreticalPrice × 建玉推定 を使用（OI列がない場合）。
    """
    pain = {}
    for k_exp in strikes:
        total = 0.0
        for _, row in df_call.iterrows():
            k = row["StrikePrice"]
            # 行使価格 k_exp 時点でコールが行使される損失
            total += float(row.get("OpenInterestProxy", 1)) * max(0, k - k_exp)
        for _, row in df_put.iterrows():
            k = row["StrikePrice"]
            total += float(row.get("OpenInterestProxy", 1)) * max(0, k_exp - k)
        pain[k_exp] = total
    if not pain:
        return strikes[len(strikes)//2]
    return min(pain, key=pain.get)


# ─────────────────────────────────────────────
# ガンマエクスポージャー (GEX)
# ─────────────────────────────────────────────

def calc_gex(df: pd.DataFrame, S: float, r: float = 0.005) -> pd.DataFrame:
    """
    行使価格ごとのガンマエクスポージャーを計算。
    GEX = Gamma × OI_proxy × S² / 100
    コールはプラス寄与、プットはマイナス寄与（ネットロングGEX基準）。
    """
    results = []
    today = date.today()
    for _, row in df.iterrows():
        K = row["StrikePrice"]
        iv = row["IV"]
        opt = row["OptionType"]
        cmonth = row["ContractMonthDt"]

        if pd.isna(K) or pd.isna(iv) or iv <= 0 or pd.isna(cmonth):
            continue

        # 残存日数（簡易: 限月第2金曜日を満期と仮定）
        exp_year, exp_month = int(cmonth.year), int(cmonth.month)
        # 第2金曜日を求める
        first = date(exp_year, exp_month, 1)
        fridays = [first + pd.Timedelta(days=d) for d in range(31)
                   if (first + pd.Timedelta(days=d)).month == exp_month
                   and (first + pd.Timedelta(days=d)).weekday() == 4]
        expiry = fridays[1] if len(fridays) >= 2 else fridays[0] if fridays else first
        T = max((expiry - today).days, 1) / 252.0

        g = bs_greeks(S, K, T, iv / 100, opt, r)
        if not g:
            continue

        oi_proxy = row.get("OpenInterestProxy", 1)
        sign = 1 if opt == "C" else -1
        gex = sign * g["gamma"] * float(oi_proxy) * S * S / 100

        results.append({
            "StrikePrice": K,
            "OptionType": opt,
            "ContractMonth": row["ContractMonth"],
            "IV": iv,
            "T_days": round((expiry - today).days),
            "Delta": g["delta"],
            "Gamma": g["gamma"],
            "Theta": g["theta"],
            "Vega": g["vega"],
            "GEX": round(gex, 2),
            "OI_proxy": oi_proxy,
        })
    return pd.DataFrame(results)


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────

def main():
    csv_path = DATA_DIR / "options_latest.csv"
    if not csv_path.exists():
        log.error("options_latest.csv が見つかりません")
        raise SystemExit(1)

    df = pd.read_csv(csv_path, dtype={"ContractMonth": str})
    df["ContractMonthDt"] = pd.to_datetime(df["ContractMonth"], format="%Y%m", errors="coerce")

    # 原資産価格
    S = float(df["UnderlyingClose"].dropna().iloc[0]) if not df["UnderlyingClose"].dropna().empty else 38000.0
    log.info("原資産価格: %.0f", S)

    # OI がない場合は TheoreticalPrice をプロキシに使用
    if "OpenInterest" not in df.columns:
        df["OpenInterestProxy"] = pd.to_numeric(df["TheoreticalPrice"], errors="coerce").fillna(1).clip(lower=1)
    else:
        df["OpenInterestProxy"] = pd.to_numeric(df["OpenInterest"], errors="coerce").fillna(1).clip(lower=1)

    # 直近限月（最も残存日数が短い）を抽出
    nearest_month = df["ContractMonth"].dropna().sort_values().iloc[0]
    df_near = df[df["ContractMonth"] == nearest_month].copy()
    df_call = df_near[df_near["OptionType"] == "C"].copy()
    df_put  = df_near[df_near["OptionType"] == "P"].copy()

    log.info("直近限月: %s  コール: %d行  プット: %d行", nearest_month, len(df_call), len(df_put))

    # 行使価格リスト
    strikes_all = sorted(df_near["StrikePrice"].dropna().unique().tolist())

    # ── マックスペイン ──
    max_pain = calc_max_pain(df_call, df_put, strikes_all)
    log.info("マックスペイン: %.0f", max_pain)

    # ── GEX ──
    gex_df = calc_gex(df_near, S)
    gex_by_strike = (
        gex_df.groupby("StrikePrice")["GEX"].sum().reset_index()
        .sort_values("StrikePrice")
    )

    # ガンマフリップ（GEX が正→負に転じる最初の行使価格）
    gex_pos = gex_by_strike[gex_by_strike["GEX"] >= 0]
    gex_neg = gex_by_strike[gex_by_strike["GEX"] < 0]
    gamma_flip = None
    if not gex_pos.empty and not gex_neg.empty:
        # 現在値付近でフリップしている最初の価格
        above = gex_by_strike[gex_by_strike["StrikePrice"] >= S]
        for i in range(len(above) - 1):
            if above.iloc[i]["GEX"] >= 0 and above.iloc[i+1]["GEX"] < 0:
                gamma_flip = float(above.iloc[i]["StrikePrice"])
                break
        if gamma_flip is None:
            below = gex_by_strike[gex_by_strike["StrikePrice"] < S].iloc[::-1]
            for i in range(len(below) - 1):
                if below.iloc[i]["GEX"] < 0 and below.iloc[i+1]["GEX"] >= 0:
                    gamma_flip = float(below.iloc[i]["StrikePrice"])
                    break

    log.info("ガンマフリップ: %s", gamma_flip)

    # ── グリークスサマリー（ATM近辺） ──
    atm_strike = min(strikes_all, key=lambda k: abs(k - S)) if strikes_all else S
    atm_call = df_call[df_call["StrikePrice"] == atm_strike]
    atm_put  = df_put[ df_put[ "StrikePrice"] == atm_strike]

    def get_atm_greeks(row_df, opt_type):
        if row_df.empty:
            return {}
        row = row_df.iloc[0]
        iv = pd.to_numeric(row["IV"], errors="coerce")
        cmonth = row.get("ContractMonthDt")
        if pd.isna(iv) or iv <= 0 or pd.isna(cmonth):
            return {}
        today = date.today()
        exp_year, exp_month = int(cmonth.year), int(cmonth.month)
        first = date(exp_year, exp_month, 1)
        fridays = [first + pd.Timedelta(days=d) for d in range(31)
                   if (first + pd.Timedelta(days=d)).month == exp_month
                   and (first + pd.Timedelta(days=d)).weekday() == 4]
        expiry = fridays[1] if len(fridays) >= 2 else fridays[0] if fridays else first
        T = max((expiry - today).days, 1) / 252.0
        return bs_greeks(S, atm_strike, T, iv / 100, opt_type)

    atm_call_greeks = get_atm_greeks(atm_call, "C")
    atm_put_greeks  = get_atm_greeks(atm_put,  "P")

    # ── 出力 JSON ──
    out = {
        "generated_at": date.today().isoformat(),
        "underlying": {
            "name": "日経225",
            "close": S,
        },
        "nearest_month": nearest_month,
        "max_pain": max_pain,
        "gamma_flip": gamma_flip,
        "atm_strike": atm_strike,
        "atm_call_greeks": atm_call_greeks,
        "atm_put_greeks":  atm_put_greeks,
        "gex_by_strike": gex_by_strike.to_dict(orient="records"),
        "strikes_summary": [
            {
                "strike": row["StrikePrice"],
                "call_iv": float(df_call[df_call["StrikePrice"]==row["StrikePrice"]]["IV"].iloc[0])
                    if not df_call[df_call["StrikePrice"]==row["StrikePrice"]].empty else None,
                "put_iv":  float(df_put[ df_put[ "StrikePrice"]==row["StrikePrice"]]["IV"].iloc[0])
                    if not df_put[ df_put[ "StrikePrice"]==row["StrikePrice"]].empty else None,
                "call_price": float(df_call[df_call["StrikePrice"]==row["StrikePrice"]]["PremiumClose"].iloc[0])
                    if not df_call[df_call["StrikePrice"]==row["StrikePrice"]].empty else None,
                "put_price":  float(df_put[ df_put[ "StrikePrice"]==row["StrikePrice"]]["PremiumClose"].iloc[0])
                    if not df_put[ df_put[ "StrikePrice"]==row["StrikePrice"]].empty else None,
            }
            for _, row in gex_by_strike.iterrows()
        ],
    }

    out_path = DATA_DIR / "analysis_latest.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    log.info("分析結果保存: %s", out_path)

    # CSV も保存
    gex_df.to_csv(DATA_DIR / "greeks_latest.csv", index=False, encoding="utf-8-sig")
    log.info("グリークスCSV保存完了")


if __name__ == "__main__":
    main()
