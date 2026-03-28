# JPX 日経225オプション 自動データ取得・分析

JPX（日本取引所グループ）のオプション理論価格等情報を毎営業日夕方に自動取得し、
グリークス・マックスペイン・ガンマエクスポージャーを計算して `data/` に保存します。

## ファイル構成

```
jpx-options/
├── .github/workflows/
│   └── fetch_jpx.yml        # GitHub Actions ワークフロー
├── scripts/
│   ├── fetch_jpx.py         # JPX CSVダウンロード・整形
│   └── compute_analysis.py  # グリークス・マックスペイン計算
├── data/                    # 自動生成（gitignore対象外）
│   ├── options_latest.csv   # 最新オプションデータ
│   ├── analysis_latest.json # 分析結果（アプリが読み込む）
│   ├── greeks_latest.csv    # グリークス一覧
│   └── meta.json            # 取得日・原資産価格など
└── README.md
```

## セットアップ手順

### 1. リポジトリを作成

```bash
# GitHub で新規リポジトリを作成後
git clone https://github.com/YOUR_USERNAME/jpx-options.git
cd jpx-options

# このファイル一式をコピーして push
git add .
git commit -m "init: JPX options auto-fetch"
git push
```

### 2. GitHub Actions の権限設定

リポジトリの **Settings → Actions → General** を開き：
- **Workflow permissions** を `Read and write permissions` に変更
- `Allow GitHub Actions to create and approve pull requests` にチェック

### 3. 動作確認（手動実行）

**Actions タブ** → `JPX Options Data Fetch` → `Run workflow` で手動実行できます。
日付を `YYYYMMDD` 形式で指定すると過去データも取得可能です。

### 4. 自動実行スケジュール

```yaml
# 毎営業日 17:30 JST（08:30 UTC）に自動実行
cron: '30 8 * * 1-5'
```

JPX のデータ更新は概ね **16:30〜17:30** なので、17:30 に設定しています。
祝日は JPX が更新しないためデータ取得はスキップされます（HTTP 404）。

---

## 出力データ仕様

### `data/options_latest.csv`

| 列名 | 内容 |
|------|------|
| OptionType | C=コール / P=プット |
| UnderlyingName | 原資産名 |
| ContractMonth | 限月 (YYYYMM) |
| StrikePrice | 行使価格 |
| PremiumClose | プレミアム終値 |
| TheoreticalPrice | 理論価格 |
| IV | インプライドボラティリティ (%) |
| UnderlyingClose | 日経225終値 |

### `data/analysis_latest.json`

```json
{
  "generated_at": "2025-11-01",
  "underlying": { "name": "日経225", "close": 38500.0 },
  "nearest_month": "202512",
  "max_pain": 38000,
  "gamma_flip": 38500,
  "atm_strike": 38500,
  "atm_call_greeks": {
    "price": 320.5,
    "delta": 0.512,
    "gamma": 0.000041,
    "theta": -15.2,
    "vega": 28.4,
    "rho": 0.0021
  },
  "atm_put_greeks": { ... },
  "gex_by_strike": [
    { "StrikePrice": 37000, "GEX": -1250000 },
    ...
  ],
  "strikes_summary": [
    { "strike": 38000, "call_iv": 18.5, "put_iv": 20.1, "call_price": 450, "put_price": 380 },
    ...
  ]
}
```

---

## アプリへの読み込み方法

```javascript
// GitHub Pages または raw コンテンツから取得
const BASE = "https://raw.githubusercontent.com/YOUR_USERNAME/jpx-options/main/data";

async function loadLatestData() {
  const [meta, analysis] = await Promise.all([
    fetch(`${BASE}/meta.json`).then(r => r.json()),
    fetch(`${BASE}/analysis_latest.json`).then(r => r.json()),
  ]);
  return { meta, analysis };
}
```

---

## 注意事項

- JPX のデータは**非商用・個人利用**を前提としてください（利用規約を確認）
- 祝日はデータが更新されないためスキップされます
- データは翌営業日分を上書きするため、過去データは `options_YYYYMMDD.csv` 形式で保存されます
