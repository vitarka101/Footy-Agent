from __future__ import annotations

import html
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT_SECONDS = 8
MAX_SEARCH_RESULTS = 5
MAX_CRAWLED_SOURCES = 4
MAX_SNIPPETS = 5
STOPWORDS = {
    "a",
    "about",
    "after",
    "all",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "this",
    "to",
    "what",
    "when",
    "which",
    "who",
    "with",
}


@dataclass(frozen=True)
class WebDocument:
    title: str
    url: str
    snippet: str
    text: str
    source_type: str


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def tokenize(text: str) -> list[str]:
    return [token for token in normalize_text(text).split() if token and token not in STOPWORDS]


def dedupe_documents(documents: list[WebDocument]) -> list[WebDocument]:
    seen: set[str] = set()
    unique: list[WebDocument] = []
    for document in documents:
        key = document.url.rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        unique.append(document)
    return unique


def unwrap_duckduckgo_url(url: str) -> str:
    parsed = urlparse(url)
    if "duckduckgo.com" not in parsed.netloc:
        return url
    params = parse_qs(parsed.query)
    if "uddg" in params and params["uddg"]:
        return unquote(params["uddg"][0])
    return url


def compact_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def is_valid_http_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def fetch_wikipedia_documents(query: str, limit: int = 3) -> list[WebDocument]:
    search_response = requests.get(
        "https://en.wikipedia.org/w/api.php",
        params={
            "action": "query",
            "list": "search",
            "srsearch": query,
            "format": "json",
            "utf8": 1,
            "srlimit": limit,
        },
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    search_response.raise_for_status()
    results = search_response.json().get("query", {}).get("search", [])

    documents: list[WebDocument] = []
    for result in results:
        title = result.get("title", "").strip()
        if not title:
            continue
        try:
            summary_response = requests.get(
                f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote(title)}",
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            summary_response.raise_for_status()
            payload = summary_response.json()
        except Exception:
            continue

        text = compact_whitespace(payload.get("extract", ""))
        url = payload.get("content_urls", {}).get("desktop", {}).get("page", "")
        if not text or not is_valid_http_url(url):
            continue
        documents.append(
            WebDocument(
                title=payload.get("title", title),
                url=url,
                snippet=text[:320],
                text=text,
                source_type="wikipedia",
            )
        )
    return documents


def search_duckduckgo(query: str, limit: int = MAX_SEARCH_RESULTS) -> list[WebDocument]:
    response = requests.get(
        "https://html.duckduckgo.com/html/",
        params={"q": query},
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    documents: list[WebDocument] = []
    for result in soup.select(".result"):
        link = result.select_one(".result__title a") or result.select_one("a.result__a")
        snippet_node = result.select_one(".result__snippet")
        if link is None:
            continue
        url = unwrap_duckduckgo_url(link.get("href", "").strip())
        if not is_valid_http_url(url):
            continue
        title = compact_whitespace(link.get_text(" ", strip=True))
        snippet = compact_whitespace(snippet_node.get_text(" ", strip=True) if snippet_node else "")
        if not title:
            continue
        documents.append(
            WebDocument(
                title=title,
                url=url,
                snippet=snippet,
                text="",
                source_type="search",
            )
        )
        if len(documents) >= limit:
            break
    return documents


def crawl_page_text(url: str) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for node in soup(["script", "style", "noscript", "svg", "header", "footer"]):
        node.decompose()

    title = compact_whitespace(soup.title.get_text(" ", strip=True) if soup.title else "")
    paragraphs = [
        compact_whitespace(paragraph.get_text(" ", strip=True))
        for paragraph in soup.find_all(["p", "li"])
    ]
    paragraphs = [paragraph for paragraph in paragraphs if len(paragraph) >= 60]
    text = " ".join(paragraphs[:18])
    text = compact_whitespace(f"{title}. {text}" if title and text else title or text)
    return text[:6000]


def hydrate_documents(search_results: list[WebDocument], crawl_limit: int = MAX_CRAWLED_SOURCES) -> list[WebDocument]:
    hydrated: list[WebDocument] = []
    for document in search_results[:crawl_limit]:
        text = document.text
        if not text:
            try:
                text = crawl_page_text(document.url)
            except Exception:
                text = document.snippet
        if not text:
            continue
        hydrated.append(
            WebDocument(
                title=document.title,
                url=document.url,
                snippet=document.snippet or text[:320],
                text=text,
                source_type="web",
            )
        )
    return hydrated


def chunk_text(text: str, chunk_size: int = 850, overlap: int = 120) -> list[str]:
    cleaned = compact_whitespace(text)
    if not cleaned:
        return []
    if len(cleaned) <= chunk_size:
        return [cleaned]

    chunks: list[str] = []
    start = 0
    while start < len(cleaned):
        end = min(len(cleaned), start + chunk_size)
        chunks.append(cleaned[start:end])
        if end >= len(cleaned):
            break
        start = max(end - overlap, start + 1)
    return chunks


def score_text(query_tokens: list[str], text: str, title: str = "") -> float:
    normalized_text = normalize_text(f"{title} {text}")
    score = 0.0
    for token in query_tokens:
        occurrences = normalized_text.count(token)
        if occurrences:
            score += 1.0 + min(2.5, occurrences * 0.35)
            if token in normalize_text(title):
                score += 1.2
    return score


def retrieve_relevant_snippets(question: str, documents: list[WebDocument], top_k: int = MAX_SNIPPETS) -> list[dict]:
    query_tokens = tokenize(question)
    ranked: list[dict] = []
    for document in documents:
        for chunk in chunk_text(document.text or document.snippet):
            score = score_text(query_tokens, chunk, document.title)
            if score <= 0:
                continue
            ranked.append(
                {
                    "title": document.title,
                    "url": document.url,
                    "source_type": document.source_type,
                    "score": round(score, 3),
                    "excerpt": chunk[:420],
                }
            )
    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:top_k]


def keyword_frequency(snippets: list[dict], top_k: int = 8) -> list[dict]:
    counts: dict[str, int] = {}
    for snippet in snippets:
        for token in tokenize(snippet.get("excerpt", "")):
            if len(token) <= 3:
                continue
            counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:top_k]
    return [{"keyword": keyword, "count": count} for keyword, count in ranked]


def build_web_fallback_bundle(question: str, search_query: str | None = None) -> dict:
    query = search_query or question
    documents: list[WebDocument] = []

    try:
        documents.extend(fetch_wikipedia_documents(query))
    except Exception:
        documents.extend([])

    try:
        documents.extend(hydrate_documents(search_duckduckgo(query)))
    except Exception:
        documents.extend([])

    documents = dedupe_documents(documents)
    snippets = retrieve_relevant_snippets(question, documents)
    if not snippets:
        raise ValueError("External football retrieval did not return any usable evidence.")

    keyword_counts = keyword_frequency(snippets)
    source_scores: dict[str, float] = {}
    source_lookup: dict[str, dict] = {}
    for snippet in snippets:
        source_scores[snippet["url"]] = source_scores.get(snippet["url"], 0.0) + float(snippet["score"])
        source_lookup.setdefault(
            snippet["url"],
            {
                "title": snippet["title"],
                "url": snippet["url"],
                "source_type": snippet["source_type"],
                "snippet": snippet["excerpt"],
            },
        )

    ranked_sources = sorted(source_scores.items(), key=lambda item: item[1], reverse=True)
    sources = [
        {
            **source_lookup[url],
            "score": round(score, 3),
        }
        for url, score in ranked_sources
    ]

    return {
        "query": query,
        "sources": sources,
        "snippets": snippets,
        "keywords": keyword_counts,
    }
