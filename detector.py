"""
ブランド分類モジュール
- ホワイトリスト照合（大手企業判定）
- 中国系OEM判定
"""

import csv
import re
from pathlib import Path

VOWELS = set("aiueoAIUEO")
WHITELIST_PATH = Path("whitelist.csv")

# アプリ起動時にホワイトリストをロードする
_whitelist: set[str] = set()


def load_whitelist() -> set[str]:
    """whitelist.csv を読み込んでブランド名のセットを返す（小文字化済み）。
    ファイルが存在しない場合は空セットを返す。
    """
    if not WHITELIST_PATH.exists():
        return set()

    brands: set[str] = set()
    try:
        with open(WHITELIST_PATH, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0].strip():
                    brands.add(row[0].strip().lower())
    except Exception:
        pass
    return brands


def reload_whitelist() -> None:
    """ホワイトリストを再読み込みする。"""
    global _whitelist
    _whitelist = load_whitelist()


# モジュールロード時に一度読み込む
_whitelist = load_whitelist()


def is_oem_brand(brand: str) -> bool:
    """中国系OEMブランドかどうかを判定する。

    Rule 1: ASCII英字のみで8文字以上、かつ母音(aiueoAIUEO)が2文字以下
    Rule 2: 大文字英字のみ4〜12文字、かつ母音が1文字以下
    """
    ascii_only = re.sub(r"[^a-zA-Z]", "", brand)

    # Rule 1: 英字のみ8文字以上 & 母音2文字以下
    if len(ascii_only) >= 8 and brand == ascii_only:
        vowel_count = sum(1 for c in ascii_only if c in VOWELS)
        if vowel_count <= 2:
            return True

    # Rule 2: 全て大文字英字のみ4〜12文字 & 母音1文字以下
    if re.fullmatch(r"[A-Z]{4,12}", brand):
        vowel_count = sum(1 for c in brand if c in VOWELS)
        if vowel_count <= 1:
            return True

    return False


def classify_brand(brand: str) -> str | None:
    """ブランドを分類する。

    Returns:
        "⚠️ 大手企業の可能性"  : ホワイトリスト一致
        "🚫 中国系OEMの可能性" : OEMパターン一致
        None                   : 通常検索が必要
    """
    if brand.lower() in _whitelist:
        return "⚠️ 大手企業の可能性"

    if is_oem_brand(brand):
        return "🚫 中国系OEMの可能性"

    return None
