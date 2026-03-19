"""
進捗管理モジュール
progress.json への読み書きで翌日再開機能を提供する
"""

import asyncio
import json
from pathlib import Path
from typing import Any

PROGRESS_FILE = Path("progress.json")
_lock = asyncio.Lock()


async def load_progress() -> dict | None:
    """progress.json を読み込む。ファイルが存在しない場合はNoneを返す。"""
    if not PROGRESS_FILE.exists():
        return None
    async with _lock:
        try:
            text = PROGRESS_FILE.read_text(encoding="utf-8")
            return json.loads(text)
        except Exception:
            return None


async def save_progress(data: dict) -> None:
    """progress.json に進捗データを保存する。"""
    async with _lock:
        PROGRESS_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


async def clear_progress() -> None:
    """progress.json を削除する。"""
    async with _lock:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()


async def checkpoint(
    brand: str,
    result: dict,
    stats: dict[str, int],
    all_brands_data: list[dict],
    results: list[dict],
) -> None:
    """1ブランド処理完了後に進捗を保存する（チェックポイント）。"""
    current = await load_progress() or {}
    processed: list[str] = current.get("processed_brands", [])
    if brand not in processed:
        processed.append(brand)

    await save_progress(
        {
            "total_brands": len(all_brands_data),
            "processed_brands": processed,
            "results": results,
            "stats": stats,
            "all_brands_data": all_brands_data,
            "completed": False,
        }
    )


async def mark_completed(
    stats: dict[str, int],
    results: list[dict],
    all_brands_data: list[dict],
) -> None:
    """全ブランド処理完了時に completed フラグを立てる。"""
    current = await load_progress() or {}
    await save_progress(
        {
            **current,
            "results": results,
            "stats": stats,
            "all_brands_data": all_brands_data,
            "completed": True,
        }
    )
