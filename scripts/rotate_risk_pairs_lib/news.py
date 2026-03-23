from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

ALIASES_PATH = Path(__file__).with_name("coin_aliases.json")

DEFAULT_FEED_URLS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://cryptoslate.com/feed/",
    "https://cryptopotato.com/feed/",
    "https://news.bitcoin.com/feed/",
)
DEFAULT_CACHE_TTL_SECONDS = 900
DEFAULT_LOOKBACK_HOURS = 24.0
DEFAULT_TIMEOUT_SECONDS = 8.0
MAX_CACHE_ENTRIES = 250

POSITIVE_KEYWORDS = {
    "surge",
    "rally",
    "gain",
    "gains",
    "breakout",
    "partnership",
    "integration",
    "approval",
    "launch",
    "bullish",
    "record",
    "adoption",
    "listing",
}
NEGATIVE_KEYWORDS = {
    "hack",
    "exploit",
    "lawsuit",
    "fraud",
    "delisting",
    "outage",
    "dump",
    "selloff",
    "bearish",
    "investigation",
    "breach",
    "liquidation",
    "scam",
}
RISK_KEYWORDS = {
    "hack": "hack",
    "exploit": "exploit",
    "delisting": "delisting",
    "lawsuit": "lawsuit",
    "fraud": "fraud",
    "breach": "breach",
    "investigation": "investigation",
    "scam": "scam",
}
CATALYST_KEYWORDS = {
    "listing",
    "launch",
    "approval",
    "partnership",
    "integration",
    "upgrade",
    "etf",
    "mainnet",
    "roadmap",
}
BUILTIN_SYMBOL_ALIASES = {
    "BTC": ("bitcoin",),
    "ETH": ("ethereum",),
    "BNB": ("binance coin", "bnb chain"),
    "SOL": ("solana",),
    "XRP": ("ripple", "xrp"),
    "DOGE": ("dogecoin",),
    "ADA": ("cardano",),
    "TRX": ("tron",),
    "LINK": ("chainlink",),
    "TAO": ("bittensor",),
    "AVAX": ("avalanche",),
    "SUI": ("sui",),
    "WLD": ("worldcoin",),
    "ZEC": ("zcash",),
    "CAKE": ("pancakeswap",),
    "FET": ("fetch.ai", "fetch ai", "asi"),
    "INJ": ("injective",),
    "NEAR": ("near protocol",),
    "LTC": ("litecoin",),
    "PEPE": ("pepe",),
    "PENGU": ("pudgy penguins", "pengu"),
    "PUMP": ("pump.fun", "pump fun"),
    "BANANAS31": ("bananas31", "banana for scale"),
    "WLFI": ("world liberty financial", "wlfi"),
    "DOGS": ("dogs",),
}
AMBIGUOUS_SYMBOLS = {"LINK", "NIGHT", "G", "U"}


def _load_symbol_aliases() -> dict[str, tuple[str, ...]]:
    merged: dict[str, list[str]] = {
        str(symbol).upper(): [str(alias).strip() for alias in aliases if str(alias).strip()]
        for symbol, aliases in BUILTIN_SYMBOL_ALIASES.items()
    }
    if ALIASES_PATH.exists():
        try:
            payload = json.loads(ALIASES_PATH.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            for symbol, aliases in payload.items():
                key = str(symbol or "").strip().upper()
                if not key:
                    continue
                alias_list = aliases if isinstance(aliases, list) else [aliases]
                bucket = merged.setdefault(key, [])
                for alias in alias_list:
                    alias_text = str(alias or "").strip()
                    if alias_text and alias_text not in bucket:
                        bucket.append(alias_text)
    return {symbol: tuple(values) for symbol, values in merged.items()}


SYMBOL_ALIASES = _load_symbol_aliases()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_cache(cache_path: Path) -> dict[str, Any]:
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_cache(cache_path: Path, payload: dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def _parse_dt(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _clean_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _fetch_feed_text(url: str, timeout_seconds: float) -> str:
    req = Request(
        url=url,
        method="GET",
        headers={
            "Accept": "application/rss+xml, application/xml, text/xml",
            "User-Agent": "crypto-bot/1.0 (+coin-news-updater)",
        },
    )
    with urlopen(req, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", errors="replace")


def _parse_feed(feed_text: str, source: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(feed_text)
    except Exception:
        return []
    entries: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = _clean_text(item.findtext("title", ""))
        description = _clean_text(item.findtext("description", ""))
        link = _clean_text(item.findtext("link", ""))
        published = (
            item.findtext("pubDate", "")
            or item.findtext("{http://purl.org/dc/elements/1.1/}date", "")
            or item.findtext("published", "")
        )
        published_at = _parse_dt(published) or _utc_now()
        if not title:
            continue
        entries.append(
            {
                "source": source,
                "title": title[:220],
                "summary": description[:320],
                "link": link[:280],
                "published_at": published_at.isoformat(),
            }
        )
    atom_ns = "{http://www.w3.org/2005/Atom}"
    for item in root.findall(f".//{atom_ns}entry"):
        title = _clean_text(item.findtext(f"{atom_ns}title", ""))
        description = _clean_text(item.findtext(f"{atom_ns}summary", "") or item.findtext(f"{atom_ns}content", ""))
        link = ""
        link_node = item.find(f"{atom_ns}link")
        if link_node is not None:
            link = _clean_text(link_node.attrib.get("href", ""))
        published = item.findtext(f"{atom_ns}updated", "") or item.findtext(f"{atom_ns}published", "")
        published_at = _parse_dt(published) or _utc_now()
        if not title:
            continue
        entries.append(
            {
                "source": source,
                "title": title[:220],
                "summary": description[:320],
                "link": link[:280],
                "published_at": published_at.isoformat(),
            }
        )
    return entries


def load_recent_news_entries(
    *,
    cache_path: Path,
    feed_urls: list[str] | tuple[str, ...] = DEFAULT_FEED_URLS,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[list[dict[str, Any]], str]:
    now = _utc_now()
    cached = _read_cache(cache_path) if cache_path.exists() else {}
    fetched_at = _parse_dt(str(cached.get("fetched_at", ""))) if cached else None
    entries = cached.get("entries", []) if isinstance(cached.get("entries", []), list) else []
    if fetched_at and (now - fetched_at).total_seconds() <= cache_ttl_seconds:
        return _filter_recent_entries(entries, lookback_hours), "cache_fresh"

    fetched_entries: list[dict[str, Any]] = []
    errors: list[str] = []
    for url in feed_urls:
        try:
            text = _fetch_feed_text(url, timeout_seconds=timeout_seconds)
            fetched_entries.extend(_parse_feed(text, source=url))
        except Exception as exc:
            errors.append(f"{url}:{type(exc).__name__}")
    if fetched_entries:
        deduped = _dedupe_entries(fetched_entries)
        payload = {"fetched_at": now.isoformat(), "entries": deduped[:MAX_CACHE_ENTRIES], "errors": errors}
        _write_cache(cache_path, payload)
        return _filter_recent_entries(payload["entries"], lookback_hours), "fetched"
    if entries:
        return _filter_recent_entries(entries, lookback_hours), "cache_stale"
    return [], "unavailable"


def _dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    ordered: list[dict[str, Any]] = []
    for entry in sorted(entries, key=lambda item: item.get("published_at", ""), reverse=True):
        key = (str(entry.get("source", "")), str(entry.get("title", "")).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        ordered.append(entry)
    return ordered


def _filter_recent_entries(entries: list[dict[str, Any]], lookback_hours: float) -> list[dict[str, Any]]:
    cutoff = _utc_now() - timedelta(hours=lookback_hours)
    recent: list[dict[str, Any]] = []
    for entry in entries:
        published_at = _parse_dt(str(entry.get("published_at", "")))
        if published_at is None or published_at < cutoff:
            continue
        recent.append(entry)
    return recent


def _keywords_for_symbol(base: str) -> list[str]:
    base = str(base or "").upper()
    keywords = [base.lower()]
    for alias in SYMBOL_ALIASES.get(base, ()):
        alias_text = alias.lower().strip()
        if not alias_text:
            continue
        keywords.append(alias_text)
        compact = re.sub(r"[^a-z0-9]+", "", alias_text)
        spaced = re.sub(r"[^a-z0-9]+", " ", alias_text).strip()
        if compact and compact != alias_text:
            keywords.append(compact)
        if spaced and spaced != alias_text:
            keywords.append(spaced)
    if base in AMBIGUOUS_SYMBOLS:
        keywords = [alias.lower() for alias in SYMBOL_ALIASES.get(base, ())]
    deduped: list[str] = []
    seen: set[str] = set()
    for keyword in keywords:
        normalized = str(keyword).strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def build_coin_news_context(pair: str, entries: list[dict[str, Any]]) -> dict[str, Any]:
    base = str(pair).split("/", 1)[0].upper()
    keywords = _keywords_for_symbol(base)
    if not keywords:
        return {
            "news_count_24h": 0,
            "sentiment": "neutral",
            "sentiment_score": 0.0,
            "major_catalyst": False,
            "risk_flags": [],
            "last_news_age_minutes": None,
            "top_headlines": [],
            "note": "no_symbol_alias",
        }

    matched: list[dict[str, Any]] = []
    for entry in entries:
        haystack = f"{entry.get('title', '')} {entry.get('summary', '')}".lower()
        if any(re.search(rf"\b{re.escape(keyword)}\b", haystack) for keyword in keywords):
            matched.append(entry)

    if not matched:
        return {
            "news_count_24h": 0,
            "sentiment": "neutral",
            "sentiment_score": 0.0,
            "major_catalyst": False,
            "risk_flags": [],
            "last_news_age_minutes": None,
            "top_headlines": [],
            "note": "no_recent_news",
        }

    score = 0.0
    risk_flags: set[str] = set()
    catalyst = False
    top_headlines: list[str] = []
    newest_minutes: int | None = None
    now = _utc_now()

    for entry in matched[:5]:
        text = f"{entry.get('title', '')} {entry.get('summary', '')}".lower()
        words = set(re.findall(r"[a-z0-9\.\-]+", text))
        score += 0.35 * len(words.intersection(POSITIVE_KEYWORDS))
        score -= 0.45 * len(words.intersection(NEGATIVE_KEYWORDS))
        if words.intersection(CATALYST_KEYWORDS):
            catalyst = True
        for keyword, label in RISK_KEYWORDS.items():
            if keyword in words:
                risk_flags.add(label)
        published_at = _parse_dt(str(entry.get("published_at", "")))
        if published_at is not None:
            age_minutes = max(0, int((now - published_at).total_seconds() // 60))
            if newest_minutes is None or age_minutes < newest_minutes:
                newest_minutes = age_minutes
        title = str(entry.get("title", "")).strip()
        if title and len(top_headlines) < 2:
            top_headlines.append(title[:120])

    sentiment = "neutral"
    if score >= 0.6:
        sentiment = "positive"
    elif score <= -0.6:
        sentiment = "negative"

    note = f"{sentiment} news backdrop"
    if risk_flags:
        note = f"risk flags: {','.join(sorted(risk_flags))}"
    elif catalyst:
        note = "recent catalyst headlines"

    return {
        "news_count_24h": len(matched),
        "sentiment": sentiment,
        "sentiment_score": round(max(-3.0, min(3.0, score)), 3),
        "major_catalyst": catalyst,
        "risk_flags": sorted(risk_flags),
        "last_news_age_minutes": newest_minutes,
        "top_headlines": top_headlines,
        "note": note[:160],
    }


def _detect_backend(target: str) -> str:
    scheme = urlparse(str(target).strip()).scheme.lower()
    return "postgres" if scheme.startswith("postgres") else "sqlite"


def _normalize_postgres_target(target: str) -> str:
    normalized = str(target).strip()
    if normalized.startswith("postgresql+psycopg2://"):
        return "postgresql://" + normalized[len("postgresql+psycopg2://") :]
    if normalized.startswith("postgresql+psycopg://"):
        return "postgresql://" + normalized[len("postgresql+psycopg://") :]
    return normalized


class CoinNewsStore:
    def __init__(self, db_target: str) -> None:
        self.db_target = str(db_target).strip()
        self.backend = _detect_backend(self.db_target)
        if self.backend == "sqlite":
            db_path = Path(self.db_target)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(str(db_path))
            self.conn.row_factory = sqlite3.Row
        else:
            import psycopg2

            self.conn = psycopg2.connect(_normalize_postgres_target(self.db_target))
        self._init_db()

    def close(self) -> None:
        self.conn.close()

    def _sql(self, query: str) -> str:
        if self.backend == "postgres":
            return query.replace("?", "%s")
        return query

    def _execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        cur = self.conn.cursor()
        cur.execute(self._sql(query), params)
        rowcount = int(cur.rowcount or 0)
        cur.close()
        return rowcount

    def _fetchall(self, query: str, params: tuple[Any, ...] = ()) -> list[Any]:
        cur = self.conn.cursor()
        cur.execute(self._sql(query), params)
        rows = cur.fetchall()
        cur.close()
        return rows

    def _init_db(self) -> None:
        id_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if self.backend == "sqlite" else "BIGSERIAL PRIMARY KEY"
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS coin_news_summaries (
                id {id_type},
                pair TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                updated_ts TEXT NOT NULL,
                news_count_24h INTEGER NOT NULL,
                sentiment TEXT NOT NULL,
                sentiment_score REAL NOT NULL,
                major_catalyst INTEGER NOT NULL,
                risk_flags_json TEXT NOT NULL,
                last_news_age_minutes INTEGER,
                note TEXT NOT NULL,
                top_headlines_json TEXT NOT NULL
            )
            """
        )
        self._execute("CREATE INDEX IF NOT EXISTS idx_coin_news_summaries_updated ON coin_news_summaries(updated_ts)")
        self.conn.commit()

    def upsert(self, pair: str, summary: dict[str, Any], updated_ts: str) -> None:
        params = (
            pair.upper(),
            pair.split("/", 1)[0].upper(),
            updated_ts,
            int(summary.get("news_count_24h", 0) or 0),
            str(summary.get("sentiment", "neutral") or "neutral"),
            float(summary.get("sentiment_score", 0.0) or 0.0),
            1 if bool(summary.get("major_catalyst", False)) else 0,
            json.dumps(summary.get("risk_flags", []) if isinstance(summary.get("risk_flags"), list) else []),
            summary.get("last_news_age_minutes"),
            str(summary.get("note", "") or "")[:160],
            json.dumps(summary.get("top_headlines", []) if isinstance(summary.get("top_headlines"), list) else []),
        )
        if self.backend == "sqlite":
            self._execute(
                """
                INSERT INTO coin_news_summaries(
                    pair, symbol, updated_ts, news_count_24h, sentiment, sentiment_score,
                    major_catalyst, risk_flags_json, last_news_age_minutes, note, top_headlines_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pair) DO UPDATE SET
                    symbol=excluded.symbol,
                    updated_ts=excluded.updated_ts,
                    news_count_24h=excluded.news_count_24h,
                    sentiment=excluded.sentiment,
                    sentiment_score=excluded.sentiment_score,
                    major_catalyst=excluded.major_catalyst,
                    risk_flags_json=excluded.risk_flags_json,
                    last_news_age_minutes=excluded.last_news_age_minutes,
                    note=excluded.note,
                    top_headlines_json=excluded.top_headlines_json
                """,
                params,
            )
        else:
            self._execute(
                """
                INSERT INTO coin_news_summaries(
                    pair, symbol, updated_ts, news_count_24h, sentiment, sentiment_score,
                    major_catalyst, risk_flags_json, last_news_age_minutes, note, top_headlines_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (pair) DO UPDATE SET
                    symbol=EXCLUDED.symbol,
                    updated_ts=EXCLUDED.updated_ts,
                    news_count_24h=EXCLUDED.news_count_24h,
                    sentiment=EXCLUDED.sentiment,
                    sentiment_score=EXCLUDED.sentiment_score,
                    major_catalyst=EXCLUDED.major_catalyst,
                    risk_flags_json=EXCLUDED.risk_flags_json,
                    last_news_age_minutes=EXCLUDED.last_news_age_minutes,
                    note=EXCLUDED.note,
                    top_headlines_json=EXCLUDED.top_headlines_json
                """,
                params,
            )

    def commit(self) -> None:
        self.conn.commit()

    def load_recent(self, pairs: list[str], max_age_minutes: int) -> dict[str, dict[str, Any]]:
        if not pairs:
            return {}
        cutoff = (_utc_now() - timedelta(minutes=max_age_minutes)).isoformat()
        placeholders = ",".join("?" for _ in pairs)
        rows = self._fetchall(
            f"""
            SELECT pair, updated_ts, news_count_24h, sentiment, sentiment_score,
                   major_catalyst, risk_flags_json, last_news_age_minutes, note, top_headlines_json
            FROM coin_news_summaries
            WHERE updated_ts >= ? AND pair IN ({placeholders})
            """,
            (cutoff, *[pair.upper() for pair in pairs]),
        )
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            pair = str(row[0]).upper()
            result[pair] = {
                "news_count_24h": int(row[2] or 0),
                "sentiment": str(row[3] or "neutral"),
                "sentiment_score": float(row[4] or 0.0),
                "major_catalyst": bool(row[5]),
                "risk_flags": json.loads(row[6] or "[]"),
                "last_news_age_minutes": row[7],
                "note": str(row[8] or ""),
                "top_headlines": json.loads(row[9] or "[]"),
            }
        return result


def refresh_coin_news_summaries(
    *,
    db_target: str,
    pairs: list[str],
    feed_urls: list[str] | tuple[str, ...] = DEFAULT_FEED_URLS,
    cache_path: Path,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    if not db_target or not pairs:
        return {"updated": 0, "status": "noop"}
    entries, status = load_recent_news_entries(
        cache_path=cache_path,
        feed_urls=feed_urls,
        cache_ttl_seconds=cache_ttl_seconds,
        lookback_hours=lookback_hours,
        timeout_seconds=timeout_seconds,
    )
    updated_ts = _utc_now().isoformat()
    store = CoinNewsStore(db_target)
    updated = 0
    try:
        for pair in pairs:
            summary = build_coin_news_context(pair, entries)
            store.upsert(pair, summary, updated_ts)
            updated += 1
        store.commit()
    finally:
        store.close()
    return {"updated": updated, "status": status, "entries": len(entries)}


def load_coin_news_contexts(
    *,
    db_target: str,
    pairs: list[str],
    max_age_minutes: int = 180,
) -> tuple[dict[str, dict[str, Any]], str]:
    if not db_target:
        return {}, "disabled"
    try:
        store = CoinNewsStore(db_target)
        try:
            rows = store.load_recent(pairs, max_age_minutes=max_age_minutes)
        finally:
            store.close()
    except Exception:
        return {}, "unavailable"
    if not rows:
        return {}, "empty"
    return rows, "db_cache"
