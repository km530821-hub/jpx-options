/**
 * jpx_loader.js
 * GitHub Actions で自動生成された JPX オプションデータを
 * オプション解析アプリに読み込むローダー。
 *
 * 使い方:
 *   const loader = new JpxLoader("YOUR_USERNAME", "jpx-options");
 *   const data = await loader.load();
 */

class JpxLoader {
  /**
   * @param {string} githubUser  - GitHubユーザー名
   * @param {string} repo        - リポジトリ名（デフォルト: "jpx-options"）
   * @param {string} branch      - ブランチ名（デフォルト: "main"）
   */
  constructor(githubUser, repo = "jpx-options", branch = "main") {
    this.base = `https://raw.githubusercontent.com/${githubUser}/${repo}/${branch}/data`;
  }

  /** 最新データをすべて取得して返す */
  async load() {
    const [meta, analysis, csv] = await Promise.all([
      this._fetchJson("meta.json"),
      this._fetchJson("analysis_latest.json"),
      this._fetchText("options_latest.csv"),
    ]);

    const options = this._parseCsv(csv);

    return {
      meta,           // 取得日・原資産価格など
      analysis,       // グリークス・マックスペイン・GEX
      options,        // 全行使価格データ
      updatedAt: meta?.date ?? null,
    };
  }

  /** アプリ向けにデータを整形して返す */
  async loadForApp() {
    const { meta, analysis, options } = await this.load();

    return {
      // ── ヘッダー情報 ──
      date: analysis.generated_at,
      underlyingClose: analysis.underlying.close,
      nearestMonth: analysis.nearest_month,

      // ── マックスペイン・ガンマフリップ ──
      maxPain: analysis.max_pain,
      gammaFlip: analysis.gamma_flip,
      atmStrike: analysis.atm_strike,

      // ── ATM グリークス ──
      atmCallGreeks: analysis.atm_call_greeks,
      atmPutGreeks:  analysis.atm_put_greeks,

      // ── 行使価格別サマリー（チャート用）──
      strikesSummary: analysis.strikes_summary,   // { strike, call_iv, put_iv, call_price, put_price }
      gexByStrike:    analysis.gex_by_strike,      // { StrikePrice, GEX }

      // ── 全データ（詳細テーブル用）──
      allOptions: options,
    };
  }

  // ── 内部メソッド ──

  async _fetchJson(filename) {
    const res = await fetch(`${this.base}/${filename}`);
    if (!res.ok) throw new Error(`Failed to fetch ${filename}: HTTP ${res.status}`);
    return res.json();
  }

  async _fetchText(filename) {
    const res = await fetch(`${this.base}/${filename}`);
    if (!res.ok) throw new Error(`Failed to fetch ${filename}: HTTP ${res.status}`);
    return res.text();
  }

  /** CSV テキストをオブジェクト配列に変換 */
  _parseCsv(text) {
    if (!text) return [];
    const lines = text.trim().split("\n");
    if (lines.length < 2) return [];

    // BOM除去
    const header = lines[0].replace(/^\uFEFF/, "").split(",").map(h => h.trim());

    return lines.slice(1).map(line => {
      const vals = line.split(",");
      const obj = {};
      header.forEach((key, i) => {
        const v = vals[i]?.trim() ?? "";
        obj[key] = isNaN(v) || v === "" ? v : Number(v);
      });
      return obj;
    });
  }
}

// ブラウザグローバルとして公開（<script> タグで読み込む場合）
if (typeof window !== "undefined") {
  window.JpxLoader = JpxLoader;
}

// Node.js / ES Module として使う場合
if (typeof module !== "undefined") {
  module.exports = { JpxLoader };
}
