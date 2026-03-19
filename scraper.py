"""
Webスクレイピングモジュール
公式サイトから会社情報（会社名・電話・FAX・メール・フォームURL）を抽出する
"""

import re
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# 会社概要ページのリンクパターン
COMPANY_PAGE_PATTERNS = [
    r"会社概要",
    r"会社情報",
    r"企業情報",
    r"企業概要",
    r"会社案内",
    r"運営会社",
    r"about[-_]?us",
    r"company",
    r"corporate",
    r"about",
]

# 問い合わせフォームのリンクパターン
CONTACT_FORM_PATTERNS = [
    r"お問い合わせ",
    r"問い合わせ",
    r"contact",
    r"inquiry",
    r"support",
    r"ご相談",
]

# 全角→半角変換テーブル
_FULLWIDTH_TABLE = str.maketrans(
    "０１２３４５６７８９－（）＋　",
    "0123456789-()+ ",
)

# 電話番号パターン（ラベル付き・高確度）
PHONE_LABELED = re.compile(
    r"(?:TEL|Tel|tel|電話番号?|お電話|T E L)[^\d０-９]{0,8}"
    r"(0\d{1,4}[-\s・．]{0,2}\d{1,4}[-\s・．]{0,2}\d{3,4})",
    re.UNICODE,
)

# FAXパターン（ラベル付き）
FAX_LABELED = re.compile(
    r"(?:FAX|Fax|fax|ファックス|ＦＡＸ|F A X)[^\d０-９]{0,8}"
    r"(0\d{1,4}[-\s・．]{0,2}\d{1,4}[-\s・．]{0,2}\d{3,4})",
    re.UNICODE,
)

# 電話番号パターン（ラベルなしフォールバック）
PHONE_BARE = re.compile(
    r"(?<!\d)(0\d{1,4}[-\s・．]{0,2}\d{1,4}[-\s・．]{0,2}\d{3,4})(?!\d)",
    re.UNICODE,
)

# メールアドレスパターン
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# 会社名パターン（法人格）
COMPANY_NAME_PATTERN = re.compile(
    r"((?:株式会社|合同会社|有限会社|一般社団法人|特定非営利活動法人)\S{1,25}"
    r"|\S{1,25}(?:株式会社|合同会社|有限会社))"
)

# スクレイピング用UA
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _normalize(text: str) -> str:
    """全角数字・記号を半角に変換する。"""
    return text.translate(_FULLWIDTH_TABLE)


def _is_same_domain(url1: str, url2: str) -> bool:
    """2つのURLが同一ドメインかどうかを確認する。"""
    try:
        return urlparse(url1).netloc == urlparse(url2).netloc
    except Exception:
        return False


def _find_company_page_url(soup: BeautifulSoup, base_url: str) -> str | None:
    """会社概要ページへのリンクを探す。"""
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        for pattern in COMPANY_PAGE_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE) or re.search(
                pattern, href, re.IGNORECASE
            ):
                candidate = urljoin(base_url, href)
                # 外部ドメインへのリンクは除外
                if _is_same_domain(candidate, base_url):
                    return candidate
    return None


def _find_contact_form_url(soup: BeautifulSoup, base_url: str) -> str | None:
    """問い合わせフォームへのリンクを探す。"""
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]
        # mailto: や # のみのリンクは除外
        if href.startswith("mailto:") or href.strip() == "#":
            continue
        for pattern in CONTACT_FORM_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE) or re.search(
                pattern, href, re.IGNORECASE
            ):
                return urljoin(base_url, href)
    return None


def _extract_company_name_from_table(soup: BeautifulSoup) -> str:
    """テーブル構造（th/td, dt/dd）から会社名を抽出する。"""
    label_patterns = ["会社名", "商号", "法人名", "屋号"]
    for cell in soup.find_all(["th", "dt"]):
        cell_text = cell.get_text(strip=True)
        if any(p in cell_text for p in label_patterns):
            # 次の兄弟要素または対応するdd/tdを取得
            sibling = cell.find_next_sibling(["td", "dd"])
            if sibling:
                name = sibling.get_text(strip=True)
                if name and len(name) <= 50:
                    return name
    return ""


def _extract_info_from_text(
    text: str,
) -> tuple[str, str, str, str]:
    """テキストから電話・FAX・メール・会社名を抽出する。

    Returns:
        (phone, fax, email, company_name)
    """
    normalized = _normalize(text)

    # FAXを先に抽出（電話との誤マッチ防止）
    fax = ""
    fax_match = FAX_LABELED.search(normalized)
    if fax_match:
        fax = fax_match.group(1)
        # FAX箇所を除去してから電話を検索
        phone_text = normalized[: fax_match.start()] + normalized[fax_match.end() :]
    else:
        phone_text = normalized

    # 電話（ラベル付き優先、フォールバックでラベルなし）
    phone = ""
    phone_match = PHONE_LABELED.search(phone_text) or PHONE_BARE.search(phone_text)
    if phone_match:
        phone = phone_match.group(1)

    # メール
    email = ""
    email_match = EMAIL_PATTERN.search(text)
    if email_match:
        # info@example.com のようなお問い合わせ用メールを優先
        email = email_match.group(0)

    # 会社名（正規表現フォールバック）
    company = ""
    company_match = COMPANY_NAME_PATTERN.search(text)
    if company_match:
        company = company_match.group(1)

    return phone, fax, email, company


async def _fetch_page(
    url: str, client: httpx.AsyncClient
) -> tuple[BeautifulSoup | None, str]:
    """URLからHTMLを取得してBeautifulSoupとレスポンステキストを返す。"""
    import asyncio
    try:
        resp = await asyncio.wait_for(
            client.get(
                url,
                headers=HEADERS,
                timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0),
                follow_redirects=True,
            ),
            timeout=20.0,  # ページ取得全体の上限（どんなに遅くても20秒で打ち切る）
        )
        if resp.status_code != 200:
            return None, ""
        soup = BeautifulSoup(resp.text, "lxml")
        return soup, resp.text
    except Exception:
        return None, ""


def _determine_flag(info: dict) -> str:
    """取得できた情報量に応じてフラグを決定する。"""
    fields = [
        info.get("会社名", ""),
        info.get("電話番号", ""),
        info.get("FAX番号", ""),
        info.get("メールアドレス", ""),
        info.get("フォームURL", ""),
    ]
    filled = sum(1 for f in fields if f)
    if filled >= 3:
        return "◎"
    elif filled >= 1:
        return "△"
    return "✕"


async def scrape_company_info(url: str, client: httpx.AsyncClient) -> dict:
    """公式サイトから会社情報を収集する。

    Returns:
        dict with keys: 会社名, HP URL, 電話番号, FAX番号, メールアドレス, フォームURL, フラグ
    """
    result = {
        "会社名": "",
        "HP URL": url,
        "電話番号": "",
        "FAX番号": "",
        "メールアドレス": "",
        "フォームURL": "",
        "フラグ": "✕",
    }

    # トップページを取得
    top_soup, top_html = await _fetch_page(url, client)
    if top_soup is None:
        return result

    # 会社概要ページを検索して取得（あれば優先的に使用）
    company_page_url = _find_company_page_url(top_soup, url)
    if company_page_url and company_page_url != url:
        company_soup, company_html = await _fetch_page(company_page_url, client)
        if company_soup is None:
            company_soup, company_html = top_soup, top_html
    else:
        company_soup, company_html = top_soup, top_html

    # テーブル構造から会社名を優先抽出
    company_name = _extract_company_name_from_table(company_soup)

    # テキスト全体から電話・FAX・メール・会社名を抽出
    full_text = company_soup.get_text(separator="\n")
    phone, fax, email, company_from_text = _extract_info_from_text(full_text)

    # 電話が見つからない場合はHTMLから直接抽出（tel: リンク）
    if not phone:
        tel_link = company_soup.find("a", href=re.compile(r"^tel:"))
        if tel_link:
            phone = tel_link["href"].replace("tel:", "").strip()

    # 会社名：テーブル抽出優先、なければ正規表現結果
    if not company_name:
        company_name = company_from_text

    # 問い合わせフォームURL（トップと会社概要ページ両方から探す）
    form_url = _find_contact_form_url(company_soup, url)
    if not form_url and company_soup is not top_soup:
        form_url = _find_contact_form_url(top_soup, url)

    # メールアドレス：HTMLソースからも直接検索（難読化対策）
    if not email:
        email_match = EMAIL_PATTERN.search(company_html)
        if email_match:
            email = email_match.group(0)

    result.update(
        {
            "会社名": company_name,
            "電話番号": phone,
            "FAX番号": fax,
            "メールアドレス": email,
            "フォームURL": form_url or "",
        }
    )
    result["フラグ"] = _determine_flag(result)
    return result
