"""
compute_analysis.py
新列構造（2026年2月24日以降）対応版
1行 = 1行使価格のコール・プット両方が入っている横持ち形式
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
    vega = S * pdf1 * sq / 100
    if opt_type == "C":
        delta = norm_cdf(d1)
        theta = (-S * pdf1 * sigma / (2 * sq) - r * K * math.exp(-r * T) * norm_cdf(d2)) / 252
        price = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
        rho = K * T * math.exp(-r * T) * norm_cdf(d2) / 100
    else:
        delta = norm_cdf(d1) - 1
        theta = (-S * pdf1 * sigma / (2 * sq) + r * K * math.exp(-r * T) * norm_cdf(-d2)) / 252
        price = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
        rho = -K * T * math.exp(-r * T) * norm_cdf(-d2) / 100
    return {"price": round(price, 2), "delta": round(delta, 4), "gamma": round(gamma, 8),
            "theta": round(theta, 2), "vega": round(vega, 4), "rho": round(rho, 4)}


def expiry_date(contract_month_dt):
    """限月の第2金曜日を返す"""
    y, m = int(contract_month_dt.year), int(contract_month_dt.month)
    first = date(y, m, 1)
    fridays = [first.replace(day=d) for d in range(1, 32)
               if first.replace(day=d).month == m and first.replace(day=d).weekday() == 4]
    return fridays[1] if len(fridays) >= 2 else fridays[0]


def calc_max_pain(df, strikes, S):
    """行使価格ごとの痛みの総和を計算しMinを返す"""
    pain = {}
    for k_exp in strikes:
        total = 0.0
        for _, row in df.iterrows():
            k = row["StrikePrice"]
            oi = max(float(row.get("OI_proxy", 1)), 1)
            total += oi * max(0, k - k_exp)      # コール側
            total += oi * max(0, k_exp - k)      # プット側
        pain[k_exp] = total
    return min(pain, key=pain.get) if pain else S


def main():
    csv_path = DATA_DIR / "options_latest.csv"
    if not csv_path.exists():
        log.error("options_latest.csv が見つかりません")
        raise SystemExit(1)

    df = pd.read_csv(csv_path, dtype={"ContractMonth": str})
    df["ContractMonthDt"] = pd.to_datetime(df["ContractMonth"], format="%Y%m", errors="coerce")

    # 原資産価格
    S = float(df["UnderlyingClose"].dropna().iloc[0]) if not df["UnderlyingClose"].dropna().empty else 38000.0
    log.info("原資産価格: %.2f", S)

    # OIプロキシ（新形式はOI列なし → TheoreticalPriceを代用）
    df["OI_proxy"] = pd.to_numeric(df["TheoreticalPrice"], errors="coerce").fillna(1).clip(lower=1)

    # 直近限月
    valid_months = df["ContractMonth"].dropna()
    if valid_months.empty:
        log.error("ContractMonth が空です")
        raise SystemExit(1)
    nearest_month = valid_months.sort_values().iloc[0]
    df_near = df[df["ContractMonth"] == nearest_month].copy()
    log.info("直近限月: %s  行数: %d", nearest_month, len(df_near))

    strikes = sorted(df_near["StrikePrice"].dropna().unique().tolist())
    if not strikes:
        log.error("行使価格が取得できません")
        raise SystemExit(1)

    # ATM
    atm_strike = min(strikes, key=lambda k: abs(k - S))

    # 満期日・残存日数
    cmonth_dt = df_near["ContractMonthDt"].dropna().iloc[0]
    exp = expiry_date(cmonth_dt)
    T = max((exp - date.today()).days, 1) / 252.0

    # ATM IV（新形式ではIVがパーセント単位かどうか確認）
    iv_raw = df_near[df_near["StrikePrice"] == atm_strike]["IV"].dropna()
    atm_iv = float(iv_raw.iloc[0]) if not iv_raw.empty else 0.25
    # IVが1未満 → すでに小数、1以上 → パーセント表記
    atm_iv_dec = atm_iv if atm_iv < 1 else atm_iv / 100

    log.info("ATM行使価格: %.0f  IV(raw): %.4f  T: %.4f年", atm_strike, atm_iv, T)

    # グリークス
    atm_call_greeks = bs_greeks(S, atm_strike, T, atm_iv_dec, "C")
    atm_put_greeks  = bs_greeks(S, atm_strike, T, atm_iv_dec, "P")

    # マックスペイン
    max_pain = calc_max_pain(df_near, strikes, S)
    log.info("マックスペイン: %.0f", max_pain)

    # GEX（簡易計算）
    gex_list = []
    for _, row in df_near.iterrows():
        K = row["StrikePrice"]
        iv = row["IV"]
        if pd.isna(K) or pd.isna(iv) or iv <= 0:
            continue
        iv_dec = iv if iv < 1 else iv / 100
        oi = float(row.get("OI_proxy", 1))
        g = bs_greeks(S, K, T, iv_dec, "C")
        if g:
            gex_list.append({"StrikePrice": K, "GEX": round(g["gamma"] * oi * S * S / 100, 2)})

    gex_df = pd.DataFrame(gex_list).groupby("StrikePrice")["GEX"].sum().reset_index()

    # ガンマフリップ
    gamma_flip = None
    above = gex_df[gex_df["StrikePrice"] >= S].reset_index(drop=True)
    for i in range(len(above) - 1):
        if above.iloc[i]["GEX"] >= 0 > above.iloc[i+1]["GEX"]:
            gamma_flip = float(above.iloc[i]["StrikePrice"])
            break

    # 行使価格別IV・価格サマリー
    strikes_summary = []
    for k in strikes:
        row = df_near[df_near["StrikePrice"] == k]
        if row.empty:
            continue
        r = row.iloc[0]
        iv_v = float(r["IV"]) if not pd.isna(r["IV"]) else None
        strikes_summary.append({
            "strike": k,
            "iv": iv_v,
            "call_close": float(r["CallClose"]) if not pd.isna(r.get("CallClose", float("nan"))) else None,
            "theoretical": float(r["TheoreticalPrice"]) if not pd.isna(r.get("TheoreticalPrice", float("nan"))) else None,
            "delta": float(r["Delta"]) if "Delta" in r and not pd.isna(r.get("Delta", float("nan"))) else None,
        })

    out = {
        "generated_at": date.today().isoformat(),
        "underlying": {"name": "日経225", "close": S},
        "nearest_month": nearest_month,
        "expiry_date": exp.isoformat(),
        "dte": (exp - date.today()).days,
        "max_pain": max_pain,
        "gamma_flip": gamma_flip,
        "atm_strike": atm_strike,
        "atm_iv_pct": round(atm_iv_dec * 100, 2),
        "atm_call_greeks": atm_call_greeks,
        "atm_put_greeks": atm_put_greeks,
        "gex_by_strike": gex_df.to_dict(orient="records"),
        "strikes_summary": strikes_summary,
    }

    out_path = DATA_DIR / "analysis_latest.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    log.info("分析結果保存: %s", out_path)

    gex_df.to_csv(DATA_DIR / "greeks_latest.csv", index=False, encoding="utf-8-sig")
    log.info("完了")


if __name__ == "__main__":
    main()
