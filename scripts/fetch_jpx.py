"""
fetch_jpx.py
2026年2月24日よりURL・形式変更:
  新URL: https://www.jpx.co.jp/automation/markets/derivatives/option-price/files/ose{YYYYMMDD}tp.csv
  形式:  ZIP圧縮 → CSV直接配信
"""

import os
import io
import json
import logging
import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.jpx.co.jp/automation/markets/derivatives/option-price/files/ose{date}tp.csv"

COLUMNS = [
    "OptionType", "UnderlyingName", "ContractMonth", "StrikePrice",
    "SecurityCode", "PremiumClose", "TheoreticalPrice", "IV",
    "UnderlyingClose", "BaseVolatility",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def prev_business_days(n: int = 5) -> list:
    days = []
    d = date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return days


def fetch_csv(target_date: str):
    url = BASE_URL.format(date=target_date)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; JPX-Options-Bot/1.0)",
        "Referer": "https://www.jpx.co.jp/markets/derivatives/option-price/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("HTTP %d: %s", r.status_code, url)
            return None
        log.info("取得成功: %s (%d bytes)", url, len(r.content))
        df = pd.read_csv(
            io.BytesIO(r.content),
            encoding="shift_jis",
            header=None,
            names=COLUMNS,
            dtype=str,
        )
        log.info("CSV読込: %d 行", len(df))
        return df
    except Exception as e:
        log.error("エラー: %s", e)
    return None


def clean(df):
    for col in ["StrikePrice", "PremiumClose", "TheoreticalPrice", "IV", "UnderlyingClose", "BaseVolatility"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[df["UnderlyingName"].str.contains("日経", na=False)].copy()
    df["ContractMonthDt"] = pd.to_datetime(df["ContractMonth"], format="%Y%m", errors="coerce")
    log.info("日経225オプション: %d 行", len(df))
    return df.reset_index(drop=True)


def main():
    target_env = os.environ.get("TARGET_DATE", "").strip()
    candidates = [target_env] if target_env else prev_business_days(5)

    df = None
    used_date = None
    for d in candidates:
        df = fetch_csv(d)
        if df is not None:
            used_date = d
            break

    if df is None:
        log.error("データ取得失敗。終了します。")
        raise SystemExit(1)

    df = clean(df)
    df.to_csv(DATA_DIR / f"options_{used_date}.csv", index=False, encoding="utf-8-sig")
    df.to_csv(DATA_DIR / "options_latest.csv", index=False, encoding="utf-8-sig")
    log.info("保存完了: options_%s.csv", used_date)

    meta = {
        "date": used_date,
        "rows": len(df),
        "underlying_close": float(df["UnderlyingClose"].dropna().iloc[0]) if not df["UnderlyingClose"].dropna().empty else None,
    }
    (DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    log.info("メタ: %s", meta)


if __name__ == "__main__":
    main()
