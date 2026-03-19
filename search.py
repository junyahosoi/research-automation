"""
Serper.dev Search API ラッパー
3ステップ検索戦略でブランドの公式サイトを検索する
"""

import re
from urllib.parse import urlparse

import httpx

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# EC・価格比較サイトのドメイン（結果から除外）
EC_DOMAINS = {
    "amazon.co.jp",
    "amazon.com",
    "rakuten.co.jp",
    "item.rakuten.co.jp",
    "shopping.yahoo.co.jp",
    "store.shopping.yahoo.co.jp",
    "mercari.com",
    "fril.jp",
    "qoo10.jp",
    "wowma.jp",
    "lohaco.jp",
    "askul.co.jp",
    "monotaro.com",
    "yodobashi.com",
    "biccamera.com",
    "kakaku.com",
    "price.com",
    "coneco.net",
    "ebay.com",
    "aliexpress.com",
    "taobao.com",
    "yahoo.co.jp",
    "zozo.jp",
    "shopify.com",
    "base.ec",
    "stores.jp",
}

# 検索クエリに付加するEC除外文字列
EC_EXCLUSION = (
    " -site:amazon.co.jp -site:rakuten.co.jp -site:yahoo.co.jp"
    " -site:mercari.com -site:kakaku.com -site:zozo.jp"
)

# 通販・ショップパスのパターン（URLパスに含まれる場合は通販ページと判断）
SHOP_PATH_PATTERNS = [
    "/shop/", "/cart/", "/products/", "/product/", "/item/",
    "/ec/", "/store/", "/buy/", "/order/", "/catalog/",
]


def is_ec_site(url: str) -> bool:
    """URLがECサイトかどうかを判定する。"""
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return any(host == ec or host.endswith("." + ec) for ec in EC_DOMAINS)
    except Exception:
        return False


def is_shop_path(url: str) -> bool:
    """URLパスが通販ページのパターンかどうかを判定する。"""
    try:
        path = urlparse(url).path.lower()
        return any(p in path for p in SHOP_PATH_PATTERNS)
    except Exception:
        return False


def to_root_url(url: str) -> str:
    """URLのルートドメインのみを返す（通販パスを除去）。"""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/"
    except Exception:
        return url


def pick_best_url(results: list[dict]) -> str | None:
    """検索結果の上位5件から最適なURLを選ぶ。
    ECサイトを除外し、通販パスのURLはルートに切り替える。
    """
    for item in results[:5]:
        url = item.get("link", "")
        if not url:
            continue
        if is_ec_site(url):
            continue
        if is_shop_path(url):
            return to_root_url(url)
        return url
    return None


def extract_japanese_keywords(product_name: str) -> str:
    """商品名からCJK文字列を抽出して再検索用キーワードを生成する。

    例: 'KKDYW 洗濯機用ホース 2m ブルー' → '洗濯機用ホース ブルー'
    """
    segments = re.findall(r"[\u3040-\u30FF\u4E00-\u9FFF]{2,}", product_name)
    return " ".join(segments[:3])


async def serper_search(
    query: str,
    api_key: str,
    client: httpx.AsyncClient,
) -> tuple[list[dict], str | None]:
    """Serper.dev APIを呼び出す。

    Returns:
        (results, error_code)
        error_code: None = 成功, "quota" = クォータ超過, "auth_error" = 認証エラー,
                    "timeout" = タイムアウト, "error" = その他エラー
    """
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "q": query,
        "gl": "jp",
        "hl": "ja",
        "num": 5,
    }
    try:
        resp = await client.post(
            SERPER_SEARCH_URL, json=payload, headers=headers, timeout=15.0
        )

        if resp.status_code == 429:
            return [], "quota"

        if resp.status_code in (401, 403):
            return [], "auth_error"

        if resp.status_code != 200:
            return [], "error"

        data = resp.json()
        items = data.get("organic", [])
        return items, None

    except httpx.TimeoutException:
        return [], "timeout"
    except Exception:
        return [], "error"


async def find_official_site(
    brand: str,
    product_name: str,
    api_key: str,
    cx: str,
    client: httpx.AsyncClient,
) -> tuple[str | None, str | None]:
    """ブランドの公式サイトURLを3ステップ戦略で検索する。

    Returns:
        (url, error_code)
        url: 公式サイトURL（見つからない場合はNone）
        error_code: None = 成功, "quota" = クォータ超過
    """
    # Step 1: "{brand} 株式会社 OR 会社概要" + EC除外
    query1 = f'"{brand}" 株式会社 OR 会社概要{EC_EXCLUSION}'
    results1, err = await serper_search(query1, api_key, client)
    if err == "quota":
        return None, "quota"

    url = pick_best_url(results1)
    if url:
        return url, None

    # Step 2: "{brand} 公式" + EC除外
    query2 = f'"{brand}" 公式{EC_EXCLUSION}'
    results2, err = await serper_search(query2, api_key, client)
    if err == "quota":
        return None, "quota"

    url = pick_best_url(results2)
    if url:
        return url, None

    # Step 3: 商品名の日本語キーワードで再検索
    jp_keywords = extract_japanese_keywords(product_name)
    if jp_keywords:
        query3 = f"{jp_keywords} メーカー 公式{EC_EXCLUSION}"
        results3, err = await serper_search(query3, api_key, client)
        if err == "quota":
            return None, "quota"

        url = pick_best_url(results3)
        if url:
            return url, None

    return None, None
