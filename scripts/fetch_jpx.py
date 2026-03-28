"""
fetch_jpx.py
JPX オプション理論価格等情報を取得し data/ に保存する。

URL形式: https://www.jpx.co.jp/markets/derivatives/option-price/data/ose{YYYYMMDD}tp.zip
CSVヘッダー: https://www.jpx.co.jp/markets/derivatives/option-price/tvdivq00000014eu-att/head.csv
"""

import os
import io
import zipfile
import logging
import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.jpx.co.jp/markets/derivatives/option-price/data/ose{date}tp.zip"

# JPX CSV 列定義 (2025年10月22日以降の仕様)
COLUMNS = [
    "OptionType",        # P=プット / C=コール
    "UnderlyingName",    # 原資産名（例: 日経225）
    "ContractMonth",     # 限月 (YYYYMM)
    "StrikePrice",       # 行使価格
    "SecurityCode",      # 銘柄コード
    "PremiumClose",      # プレミアム終値
    "TheoreticalPrice",  # 理論価格
    "IV",                # ボラティリティ（清算価格算出時）
    "UnderlyingClose",   # 原資産終値
    "BaseVolatility",    # 基準ボラティリティ
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


def prev_business_days(n: int = 5) -> list[str]:
    """直近 n 営業日の日付リストを返す (新→旧)"""
    days = []
    d = date.today()
    while len(days) < n:
        if d.weekday() < 5:          # 月〜金
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return days


def fetch_zip(target_date: str) -> bytes | None:
    url = BASE_URL.format(date=target_date)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; JPX-Options-Bot/1.0; "
            "+https://github.com/YOUR_USERNAME/jpx-options)"
        ),
        "Referer": "https://www.jpx.co.jp/markets/derivatives/option-price/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            log.info("取得成功: %s (%d bytes)", url, len(r.content))
            return r.content
        log.warning("HTTP %d: %s", r.status_code, url)
    except requests.RequestException as e:
        log.error("接続エラー: %s", e)
    return None


def parse_zip(raw: bytes) -> pd.DataFrame | None:
    """ZIP内CSVを読み込み DataFrame を返す"""
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                log.error("ZIP内にCSVなし")
                return None
            with zf.open(csv_names[0]) as f:
                df = pd.read_csv(
                    f,
                    encoding="shift_jis",
                    header=None,
                    names=COLUMNS,
                    dtype=str,
                )
        log.info("CSV読込: %d 行", len(df))
        return df
    except Exception as e:
        log.error("ZIP解析エラー: %s", e)
        return None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """型変換・フィルタリング"""
    # 数値変換
    for col in ["StrikePrice", "PremiumClose", "TheoreticalPrice",
                "IV", "UnderlyingClose", "BaseVolatility"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 日経225オプションのみ抽出
    mask = df["UnderlyingName"].str.contains("日経", na=False)
    df = df[mask].copy()

    # 限月を日付型に
    df["ContractMonthDt"] = pd.to_datetime(df["ContractMonth"], format="%Y%m", errors="coerce")

    log.info("日経225オプション: %d 行", len(df))
    return df.reset_index(drop=True)


def main():
    # 環境変数で日付指定可（GitHub Actions の workflow_dispatch 用）
    target_env = os.environ.get("TARGET_DATE", "").strip()
    candidates = [target_env] if target_env else prev_business_days(5)

    df = None
    used_date = None
    for d in candidates:
        raw = fetch_zip(d)
        if raw:
            df = parse_zip(raw)
            if df is not None:
                used_date = d
                break

    if df is None:
        log.error("データ取得失敗。終了します。")
        raise SystemExit(1)

    df = clean(df)

    # 保存
    out_csv = DATA_DIR / f"options_{used_date}.csv"
    latest_csv = DATA_DIR / "options_latest.csv"

    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    log.info("保存完了: %s", out_csv)

    # メタ情報
    meta = {
        "date": used_date,
        "rows": len(df),
        "underlying_close": float(df["UnderlyingClose"].dropna().iloc[0])
        if not df["UnderlyingClose"].dropna().empty else None,
    }
    import json
    (DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    log.info("メタ: %s", meta)


if __name__ == "__main__":
    main()
