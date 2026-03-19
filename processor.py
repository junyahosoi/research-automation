"""
コアパイプラインモジュール
CSVパース・重複除去・ブランド処理（検索・スクレイピング）を担う
"""

import asyncio
import csv
import io
from typing import AsyncGenerator

import httpx

from detector import classify_brand
from progress_store import checkpoint, mark_completed
from scraper import scrape_company_info
from search import find_official_site

# 必須列名
REQUIRED_COLUMNS = {"商品名", "URL: Amazon", "ASIN", "ブランド"}

# 統計キー
STAT_WHITELIST = "whitelist"
STAT_OEM = "oem"
STAT_SUCCESS = "success"
STAT_FAIL = "fail"


def parse_and_deduplicate(file_bytes: bytes) -> tuple[list[dict], int]:
    """UTF-8 BOM付きCSVをパースして重複除去する。

    Args:
        file_bytes: アップロードされたCSVのバイトデータ

    Returns:
        (deduplicated_rows, original_count)
        deduplicated_rows: ブランド列で重複除去した行リスト（初出優先）
        original_count: 除去前の行数

    Raises:
        ValueError: 必須列が不足している場合
    """
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        # BOMなしUTF-8にフォールバック
        try:
            text = file_bytes.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError(
                "CSVファイルの文字コードを読み込めませんでした。UTF-8（BOMあり）で保存してください。"
            )

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSVファイルが空です。")

    # 必須列チェック
    existing = set(reader.fieldnames)
    missing = REQUIRED_COLUMNS - existing
    if missing:
        raise ValueError(
            f"以下の必須列が見つかりません: {', '.join(sorted(missing))}"
        )

    all_rows = list(reader)
    original_count = len(all_rows)

    # ブランド列で重複除去（初出優先、空欄は「ブランド不明」として扱う）
    seen: dict[str, dict] = {}
    for row in all_rows:
        brand = row.get("ブランド", "").strip()
        if not brand:
            brand = "ブランド不明"
            row = dict(row)
            row["ブランド"] = brand
        if brand not in seen:
            seen[brand] = row

    return list(seen.values()), original_count


FLAG_REASONS = {
    "⚠️ 大手企業の可能性": "ホワイトリスト該当",
    "🚫 中国系OEMの可能性": "OEMパターン該当",
    "⛔ ブランド情報なし": "ノーブランド系スキップ",
    "◎": "3項目以上取得",
    "△": "1〜2項目取得",
    "✕": "公式サイト未発見",
}


def _make_empty_result(brand: str, flag: str, row: dict, reason: str | None = None) -> dict:
    """空の結果を生成する。Keepa元データを含む。"""
    return {
        "フラグ": flag,
        "理由": reason or FLAG_REASONS.get(flag, ""),
        "ブランド": brand,
        "商品名": row.get("商品名", ""),
        "売れ筋ランキング: 現在価格": row.get("売れ筋ランキング: 現在価格", ""),
        "Buy Box: 現在価格": row.get("Buy Box: 現在価格", ""),
        "ASIN": row.get("ASIN", ""),
        "URL: Amazon": row.get("URL: Amazon", ""),
        "会社名": "",
        "HP URL": "",
        "電話番号": "",
        "FAX番号": "",
        "メールアドレス": "",
        "フォームURL": "",
    }


async def run_pipeline(
    brands_data: list[dict],
    api_key: str,
    cx: str,
    resume_from: set[str],
    results: list[dict],
) -> AsyncGenerator[dict, None]:
    """ブランド処理パイプライン（非同期ジェネレータ）。

    各ブランドを処理してSSEイベントdictをyieldする。

    Args:
        brands_data: 重複除去済みのブランド行リスト
        api_key: Google Custom Search APIキー
        cx: 検索エンジンID
        resume_from: すでに処理済みのブランド名セット（再開時）
        results: 既存の結果リスト（再開時に引き継ぐ）

    Yields:
        dict: SSEイベント
            - type="searching": 検索中 {brand, stats}
            - type="result": 処理完了 {brand, flag, stats}
            - type="quota": クォータ超過 {stats, processed, total}
            - type="done": 全件完了 {stats}
    """
    stats = {
        STAT_WHITELIST: 0,
        STAT_OEM: 0,
        STAT_SUCCESS: 0,
        STAT_FAIL: 0,
    }

    # 再開時は既存の統計を引き継ぐ（簡易カウント）
    for r in results:
        flag = r.get("フラグ", "")
        if flag == "⚠️ 大手企業の可能性":
            stats[STAT_WHITELIST] += 1
        elif flag == "🚫 中国系OEMの可能性":
            stats[STAT_OEM] += 1
        elif flag in ("◎", "△"):
            stats[STAT_SUCCESS] += 1
        elif flag == "✕":
            stats[STAT_FAIL] += 1

    # 検索用とスクレイピング用でクライアントを分離（接続プール汚染を防ぐ）
    search_client = httpx.AsyncClient()
    async with search_client:
        for row in brands_data:
            brand = row.get("ブランド", "").strip() or "ブランド不明"
            product_name = row.get("商品名", "").strip()

            # 再開時はスキップ
            if brand in resume_from:
                continue

            # --- 分類判定 ---
            flag = classify_brand(brand)

            if flag == "⛔ ブランド情報なし":
                stats[STAT_FAIL] += 1
                result = _make_empty_result(brand, flag, row)
                results.append(result)
                await checkpoint(brand, result, dict(stats), brands_data, results)
                yield {"type": "result", "brand": brand, "flag": flag, "stats": dict(stats)}
                continue

            if flag == "⚠️ 大手企業の可能性":
                stats[STAT_WHITELIST] += 1
                result = _make_empty_result(brand, flag, row)
                results.append(result)
                await checkpoint(brand, result, dict(stats), brands_data, results)
                yield {"type": "result", "brand": brand, "flag": flag, "stats": dict(stats)}
                continue

            if flag == "🚫 中国系OEMの可能性":
                stats[STAT_OEM] += 1
                result = _make_empty_result(brand, flag, row)
                results.append(result)
                await checkpoint(brand, result, dict(stats), brands_data, results)
                yield {"type": "result", "brand": brand, "flag": flag, "stats": dict(stats)}
                continue

            # --- 検索 ---
            yield {"type": "searching", "brand": brand, "stats": dict(stats)}

            official_url, error = await find_official_site(
                brand, product_name, api_key, cx, search_client
            )

            if error == "quota":
                processed_count = len(results)
                remaining = len(brands_data) - processed_count
                yield {
                    "type": "quota",
                    "stats": dict(stats),
                    "processed": processed_count,
                    "remaining": remaining,
                }
                return

            if not official_url:
                stats[STAT_FAIL] += 1
                result = _make_empty_result(brand, "✕", row, reason="公式サイト未発見")
            else:
                # --- スクレイピング（ブランドごとに新しいクライアントを使用）---
                async with httpx.AsyncClient() as scrape_client:
                    scraped = await scrape_company_info(official_url, scrape_client)
                scraped_flag = scraped.get("フラグ", "✕")
                reason = FLAG_REASONS.get(scraped_flag, "情報取得0件") if scraped_flag != "✕" else "情報取得0件"
                result = {
                    "フラグ": scraped_flag,
                    "理由": reason,
                    "ブランド": brand,
                    "商品名": row.get("商品名", ""),
                    "売れ筋ランキング: 現在価格": row.get("売れ筋ランキング: 現在価格", ""),
                    "Buy Box: 現在価格": row.get("Buy Box: 現在価格", ""),
                    "ASIN": row.get("ASIN", ""),
                    "URL: Amazon": row.get("URL: Amazon", ""),
                    "会社名": scraped.get("会社名", ""),
                    "HP URL": scraped.get("HP URL", ""),
                    "電話番号": scraped.get("電話番号", ""),
                    "FAX番号": scraped.get("FAX番号", ""),
                    "メールアドレス": scraped.get("メールアドレス", ""),
                    "フォームURL": scraped.get("フォームURL", ""),
                }
                if scraped_flag in ("◎", "△"):
                    stats[STAT_SUCCESS] += 1
                else:
                    stats[STAT_FAIL] += 1

            results.append(result)
            await checkpoint(brand, result, dict(stats), brands_data, results)
            yield {
                "type": "result",
                "brand": brand,
                "flag": result["フラグ"],
                "stats": dict(stats),
            }

            # スクレイピング対象サイトへの丁寧なアクセス間隔
            await asyncio.sleep(1.0)

    await mark_completed(dict(stats), results, brands_data)
    yield {"type": "done", "stats": dict(stats)}
