import json
import math
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from app.storage import PredictionStore


ENTRY_DIAG_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?Entry diag (?P<payload>.+)$")
ENTRY_DIAG_CACHE: dict[str, object] = {"path": None, "mtime_ns": None, "size": None, "rows": []}
ROTATION_LOG_CACHE: dict[str, object] = {"path": None, "mtime_ns": None, "size": None, "rows": []}
DAILY_GUARD_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}


def _esc(value) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _fmt_pct(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.2f}%"


def _fmt_num(value, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


def _fmt_money(value, currency: str = "USDT", digits: int = 2) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f} {currency}"


def _normalize_utc_iso(value) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3,6}", text):
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
    except ValueError:
        return ""
    return parsed.astimezone(timezone.utc).isoformat()


def _utc_time_html(value, fallback=None) -> str:
    ts_iso = _normalize_utc_iso(value)
    display = str(fallback if fallback is not None else value or "")
    if not ts_iso:
        return _esc(display)
    escaped_iso = _esc(ts_iso)
    text = _esc(display or ts_iso)
    return (
        f'<time class="js-local-ts" datetime="{escaped_iso}" data-utc-ts="{escaped_iso}" '
        f'title="UTC {escaped_iso}">{text}</time>'
    )


def _clip_text(value, max_chars: int = 180) -> str:
    text = "" if value is None else str(value)
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}... ({len(text)} chars)"


def _code_cell_html(value) -> str:
    text = "" if value is None else str(value)
    escaped_full = _esc(text)
    escaped_preview = _esc(_clip_text(text, 180))
    if len(text) <= 180:
        return f'<div class="code-scroll">{escaped_full}</div>'
    return (
        '<details class="code-details">'
        f'<summary class="code-summary">{escaped_preview}</summary>'
        f'<div class="code-scroll">{escaped_full}</div>'
        "</details>"
    )


def _fmt_llm_allowed(value) -> str:
    if value is None:
        return "unknown"
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if parsed == 1:
        return "allowed"
    if parsed == 0:
        return "blocked"
    return "unknown"


def _rotation_news_summary(row: dict[str, object]) -> str:
    parts = [
        str(int(row.get("news_count_24h") or 0)),
        str(row.get("news_sentiment") or "").strip(),
        str(row.get("news_risk_flags") or "").strip(),
    ]
    return _clip_text(" ".join(part for part in parts if part).strip(), 80)


def _parse_int(raw_value, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _shared_bot_api_url() -> str:
    value = os.getenv("LLM_BOT_API_URL")
    if value and value.strip():
        return value.strip()
    value = os.getenv("BOT_API_URL")
    if value and value.strip():
        return value.strip()
    return "http://bot-api:8000"


def _normalize_symbol(raw_value) -> str | None:
    symbol = str(raw_value or "").strip().upper()
    if not symbol:
        return None
    return symbol[:30]


def _parse_llm_verdict(raw_value) -> int | None:
    value = str(raw_value or "").strip().lower()
    if value in {"", "all"}:
        return None
    if value in {"1", "allowed"}:
        return 1
    if value in {"0", "blocked"}:
        return 0
    if value in {"-1", "unknown"}:
        return -1
    return None


def _parse_outcomes_status(raw_value) -> str | None:
    value = str(raw_value or "").strip().lower()
    if value in {"pending", "resolved"}:
        return value
    return None


def _parse_date(raw_value) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        return ""


def _date_bounds(date_from: str, date_to: str) -> tuple[str | None, str | None]:
    ts_from: str | None = None
    ts_to: str | None = None
    if date_from:
        start = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        ts_from = start.isoformat()
    if date_to:
        end = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        ts_to = end.isoformat()
    return ts_from, ts_to


def _query_href(request: web.Request, updates: dict[str, str | int], drop: set[str] | None = None) -> str:
    query = dict(request.query)
    for key in drop or set():
        query.pop(key, None)
    for key, value in updates.items():
        query[key] = str(value)
    return str(request.rel_url.with_query(query))


def _hidden_query_inputs(request: web.Request, exclude: set[str]) -> str:
    parts: list[str] = []
    for key, value in request.query.items():
        if key in exclude:
            continue
        parts.append(f'<input type="hidden" name="{_esc(key)}" value="{_esc(value)}" />')
    return "".join(parts)


def _total_pages(total_rows: int, page_size: int) -> int:
    if total_rows <= 0:
        return 1
    return int(math.ceil(total_rows / float(page_size)))


def _safe_float(value: str | None) -> float | None:
    try:
        return float(value) if value not in {None, ""} else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in {None, ""} else None
    except (TypeError, ValueError):
        return None


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


def _coin_news_db_target() -> str:
    return str(os.getenv("COIN_NEWS_DB_URL", "")).strip()


def _freqtrade_db_target() -> str:
    value = str(os.getenv("FREQTRADE_DB_URL", "")).strip()
    if value:
        return value
    value = str(os.getenv("LLM_POLICY_TRADES_DB", "")).strip()
    if value:
        return value
    return ""


def _freqtrade_api_base_url() -> str:
    value = str(os.getenv("FREQTRADE_API_URL", "")).strip()
    if value:
        return value.rstrip("/")
    return "http://freqtrade:8080"


def _freqtrade_api_auth() -> aiohttp.BasicAuth | None:
    username = str(os.getenv("FREQTRADE_API_USERNAME", "")).strip()
    password = str(os.getenv("FREQTRADE_API_PASSWORD", "")).strip()
    if not username:
        return None
    return aiohttp.BasicAuth(login=username, password=password)


def _daily_pnl_base_mode() -> str:
    value = str(os.getenv("DAILY_PNL_BASE_MODE", "config")).strip().lower()
    if value not in {"fixed", "config", "equity"}:
        return "config"
    return value


def _daily_target_pct_value() -> float:
    return _safe_float(os.getenv("DAILY_TARGET_PCT")) or 1.0


def _daily_max_drawdown_pct_value() -> float:
    return _safe_float(os.getenv("DAILY_MAX_DRAWDOWN_PCT")) or -1.5


def _daily_pnl_fixed_capital_value() -> float:
    return _safe_float(os.getenv("DAILY_PNL_BASE_CAPITAL")) or 200.0


def _stake_total_balance_pct_value() -> float:
    return _safe_float(os.getenv("STAKE_TOTAL_BALANCE_PCT")) or 0.0


def _daily_target_switch_to_defensive_value() -> bool:
    return _env_bool("DAILY_TARGET_SWITCH_TO_DEFENSIVE", True)


def _daily_guard_capital_from_balance(balance_payload: dict[str, object]) -> float | None:
    for key in ("total_bot", "total", "starting_capital"):
        value = _safe_float(str(balance_payload.get(key, "")))
        if value is not None and value > 0.0:
            return value
    return None


def _load_daily_realized_trade_stats() -> dict[str, object]:
    target = _freqtrade_db_target()
    stats: dict[str, object] = {
        "realized_abs": 0.0,
        "closed_trades_today": 0,
        "db_error": "",
    }
    if not target:
        stats["db_error"] = "freqtrade_db_unset"
        return stats

    backend = _detect_backend(target)
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        if backend == "postgres":
            import psycopg2
            import psycopg2.extras

            conn = psycopg2.connect(_normalize_postgres_target(target))
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT
                    COUNT(*) AS closed_trades,
                    COALESCE(SUM(COALESCE(close_profit_abs, stake_amount * close_profit)), 0.0) AS realized_abs
                FROM trades
                WHERE is_open = false
                  AND close_date IS NOT NULL
                  AND close_date >= %s
                """,
                (day_start,),
            )
            row = dict(cur.fetchone() or {})
            cur.close()
            conn.close()
        else:
            import sqlite3

            db_path = target
            if db_path.startswith("sqlite:///"):
                db_path = db_path[len("sqlite:///") :]
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            row = dict(
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS closed_trades,
                        COALESCE(SUM(COALESCE(close_profit_abs, stake_amount * close_profit)), 0.0) AS realized_abs
                    FROM trades
                    WHERE is_open = 0
                      AND close_date IS NOT NULL
                      AND julianday(close_date) >= julianday(?)
                    """,
                    (day_start.isoformat(),),
                ).fetchone()
                or {}
            )
            cur.close()
            conn.close()
        stats["realized_abs"] = float(row.get("realized_abs") or 0.0)
        stats["closed_trades_today"] = int(row.get("closed_trades") or 0)
    except Exception as exc:
        stats["db_error"] = f"db_error:{str(exc)[:120]}"
    return stats


def _load_bot_performance_snapshot(hours: int) -> dict[str, object]:
    target = _freqtrade_db_target()
    stats: dict[str, object] = {
        "hours": hours,
        "closed_trades": 0,
        "win_rate_pct": 0.0,
        "avg_profit_pct": 0.0,
        "net_profit_abs": 0.0,
        "best_trade_abs": 0.0,
        "worst_trade_abs": 0.0,
        "best_pair": "",
        "concentration_pct": 0.0,
        "db_error": "",
    }
    if not target:
        stats["db_error"] = "freqtrade_db_unset"
        return stats

    backend = _detect_backend(target)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    try:
        if backend == "postgres":
            import psycopg2
            import psycopg2.extras

            conn = psycopg2.connect(_normalize_postgres_target(target))
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT
                    COUNT(*) AS closed_trades,
                    COALESCE(AVG(close_profit * 100.0), 0.0) AS avg_profit_pct,
                    COALESCE(SUM(close_profit_abs), 0.0) AS net_profit_abs,
                    COALESCE(100.0 * AVG(CASE WHEN close_profit > 0 THEN 1.0 ELSE 0.0 END), 0.0) AS win_rate_pct,
                    COALESCE(MAX(close_profit_abs), 0.0) AS best_trade_abs,
                    COALESCE(MIN(close_profit_abs), 0.0) AS worst_trade_abs
                FROM trades
                WHERE is_open = false
                  AND close_date IS NOT NULL
                  AND close_date >= %s
                """,
                (cutoff,),
            )
            summary = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT pair, close_profit_abs
                FROM trades
                WHERE is_open = false
                  AND close_date IS NOT NULL
                  AND close_date >= %s
                ORDER BY close_profit_abs DESC NULLS LAST
                LIMIT 1
                """,
                (cutoff,),
            )
            best_row = dict(cur.fetchone() or {})
            cur.close()
            conn.close()
        else:
            import sqlite3

            db_path = target
            if db_path.startswith("sqlite:///"):
                db_path = db_path[len("sqlite:///") :]
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            summary = dict(
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS closed_trades,
                        COALESCE(AVG(close_profit * 100.0), 0.0) AS avg_profit_pct,
                        COALESCE(SUM(close_profit_abs), 0.0) AS net_profit_abs,
                        COALESCE(100.0 * AVG(CASE WHEN close_profit > 0 THEN 1.0 ELSE 0.0 END), 0.0) AS win_rate_pct,
                        COALESCE(MAX(close_profit_abs), 0.0) AS best_trade_abs,
                        COALESCE(MIN(close_profit_abs), 0.0) AS worst_trade_abs
                    FROM trades
                    WHERE is_open = 0
                      AND close_date IS NOT NULL
                      AND julianday(close_date) >= julianday(?)
                    """,
                    (cutoff.isoformat(),),
                ).fetchone()
                or {}
            )
            best_row = dict(
                cur.execute(
                    """
                    SELECT pair, close_profit_abs
                    FROM trades
                    WHERE is_open = 0
                      AND close_date IS NOT NULL
                      AND julianday(close_date) >= julianday(?)
                    ORDER BY close_profit_abs DESC
                    LIMIT 1
                    """,
                    (cutoff.isoformat(),),
                ).fetchone()
                or {}
            )
            cur.close()
            conn.close()
        stats["closed_trades"] = int(summary.get("closed_trades") or 0)
        stats["avg_profit_pct"] = float(summary.get("avg_profit_pct") or 0.0)
        stats["net_profit_abs"] = float(summary.get("net_profit_abs") or 0.0)
        stats["win_rate_pct"] = float(summary.get("win_rate_pct") or 0.0)
        stats["best_trade_abs"] = float(summary.get("best_trade_abs") or 0.0)
        stats["worst_trade_abs"] = float(summary.get("worst_trade_abs") or 0.0)
        stats["best_pair"] = str(best_row.get("pair") or "")
        if abs(stats["net_profit_abs"]) > 1e-9:
            stats["concentration_pct"] = abs(stats["best_trade_abs"]) / abs(stats["net_profit_abs"]) * 100.0
    except Exception as exc:
        stats["db_error"] = f"db_error:{str(exc)[:120]}"
    return stats


async def _fetch_freqtrade_api_json(path: str) -> tuple[dict[str, object] | list[object] | None, str | None]:
    base_url = _freqtrade_api_base_url()
    auth = _freqtrade_api_auth()
    url = f"{base_url}{path}"
    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout, auth=auth) as session:
            async with session.get(url) as response:
                if response.status >= 400:
                    return None, f"http_{response.status}"
                payload = await response.json()
                return payload, None
    except Exception as exc:
        return None, str(exc)[:120]


async def _daily_guard_snapshot() -> dict[str, object]:
    now = time.time()
    cached_ts = float(DAILY_GUARD_CACHE.get("ts") or 0.0)
    cached_payload = DAILY_GUARD_CACHE.get("payload")
    if isinstance(cached_payload, dict) and (now - cached_ts) < 15.0:
        return dict(cached_payload)

    trade_stats = _load_daily_realized_trade_stats()
    balance_payload, balance_error = await _fetch_freqtrade_api_json("/api/v1/balance")
    status_payload, status_error = await _fetch_freqtrade_api_json("/api/v1/status")

    mode = _daily_pnl_base_mode()
    target_pct = _daily_target_pct_value()
    max_loss_pct = _daily_max_drawdown_pct_value()
    fixed_capital = _daily_pnl_fixed_capital_value()
    base_capital = fixed_capital
    base_source = "fixed_capital"

    balance_dict = balance_payload if isinstance(balance_payload, dict) else {}
    if mode == "equity":
        equity_capital = _daily_guard_capital_from_balance(balance_dict)
        if equity_capital is not None and equity_capital > 0.0:
            base_capital = equity_capital
            base_source = "live_equity"
        else:
            base_source = "live_equity_unavailable"
    elif mode == "config":
        config_capital = _safe_float(os.getenv("DAILY_PNL_BASE_CAPITAL")) or fixed_capital
        base_capital = config_capital
        base_source = "configured_capital"

    realized_abs = float(trade_stats.get("realized_abs") or 0.0)
    realized_pct = (realized_abs / base_capital) * 100.0 if base_capital > 0 else 0.0
    target_abs = base_capital * (target_pct / 100.0)
    max_loss_abs = base_capital * (max_loss_pct / 100.0)
    allow_entries = True
    reason = "ok"
    if realized_pct >= target_pct:
        if _daily_target_switch_to_defensive_value():
            allow_entries = True
            reason = "daily_target_defensive"
        else:
            allow_entries = False
            reason = "daily_target_reached"
    elif realized_pct <= max_loss_pct:
        allow_entries = False
        reason = "daily_loss_limit"

    open_trades = len(status_payload) if isinstance(status_payload, list) else None
    total_bot = _safe_float(str(balance_dict.get("total_bot", "")))
    total_balance = _safe_float(str(balance_dict.get("total", "")))
    starting_capital = _safe_float(str(balance_dict.get("starting_capital", "")))

    payload = {
        "mode": mode,
        "base_capital": base_capital,
        "base_source": base_source,
        "realized_abs": realized_abs,
        "realized_pct": realized_pct,
        "target_pct": target_pct,
        "target_abs": target_abs,
        "max_loss_pct": max_loss_pct,
        "max_loss_abs": max_loss_abs,
        "allow_entries": allow_entries,
        "reason": reason,
        "closed_trades_today": int(trade_stats.get("closed_trades_today") or 0),
        "open_trades": open_trades,
        "total_bot": total_bot,
        "total_balance": total_balance,
        "starting_capital": starting_capital,
        "stake_total_balance_pct": _stake_total_balance_pct_value(),
        "api_error": balance_error or status_error or "",
        "db_error": str(trade_stats.get("db_error") or ""),
    }
    DAILY_GUARD_CACHE["ts"] = now
    DAILY_GUARD_CACHE["payload"] = dict(payload)
    return payload


def _load_coin_news_rows(limit: int = 20) -> list[dict[str, object]]:
    target = _coin_news_db_target()
    if not target:
        return []
    backend = _detect_backend(target)
    rows: list[dict[str, object]] = []
    try:
        if backend == "postgres":
            import psycopg2
            import psycopg2.extras

            conn = psycopg2.connect(_normalize_postgres_target(target))
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT pair, updated_ts, news_count_24h, sentiment, major_catalyst,
                       risk_flags_json, last_news_age_minutes, note, top_headlines_json
                FROM coin_news_summaries
                ORDER BY news_count_24h DESC, updated_ts DESC, pair ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = [dict(row) for row in cur.fetchall() or []]
            cur.close()
            conn.close()
        else:
            import sqlite3

            conn = sqlite3.connect(target)
            conn.row_factory = sqlite3.Row
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT pair, updated_ts, news_count_24h, sentiment, major_catalyst,
                           risk_flags_json, last_news_age_minutes, note, top_headlines_json
                    FROM coin_news_summaries
                    ORDER BY news_count_24h DESC, updated_ts DESC, pair ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]
            conn.close()
    except Exception:
        return []

    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                "pair": str(row.get("pair") or ""),
                "updated_ts": str(row.get("updated_ts") or ""),
                "news_count_24h": int(row.get("news_count_24h") or 0),
                "sentiment": str(row.get("sentiment") or ""),
                "major_catalyst": bool(row.get("major_catalyst")),
                "risk_flags": ", ".join(json.loads(str(row.get("risk_flags_json") or "[]"))),
                "last_news_age_minutes": row.get("last_news_age_minutes"),
                "note": str(row.get("note") or ""),
                "headlines": " | ".join(json.loads(str(row.get("top_headlines_json") or "[]"))),
            }
        )
    return normalized


def _entry_diag_status(base_ok: int | None, final_ok: int | None) -> str:
    if final_ok == 1:
        return "entry"
    if base_ok == 1 and final_ok == 0:
        return "llm_blocked"
    return "rule_blocked"


def _parse_entry_diag_line(raw_line: str) -> dict[str, object] | None:
    match = ENTRY_DIAG_LINE_RE.search(raw_line.strip())
    if match is None:
        return None

    payload = match.group("payload")
    fields: dict[str, str] = {}
    for token in payload.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = value

    pair = fields.get("pair", "").upper()
    candle = fields.get("candle", "")
    if not pair or not candle:
        return None

    ts_raw = match.group("ts")
    try:
        ts_iso = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        ts_iso = ""

    base_ok = _safe_int(fields.get("base_ok"))
    final_ok = _safe_int(fields.get("final_ok"))
    failed = fields.get("failed", "")

    return {
        "ts": ts_raw,
        "ts_iso": ts_iso,
        "pair": pair,
        "candle": candle,
        "base_ok": base_ok,
        "final_ok": final_ok,
        "status": _entry_diag_status(base_ok, final_ok),
        "tag": fields.get("tag", ""),
        "failed": failed,
        "rsi": _safe_float(fields.get("rsi")),
        "adx": _safe_float(fields.get("adx")),
        "atr_pct": _safe_float(fields.get("atr_pct")),
        "spread": _safe_float(fields.get("spread")),
        "vol_z": _safe_float(fields.get("vol_z")),
        "bench_risk_ok": _safe_int(fields.get("bench_risk_ok")),
        "th_rsi": fields.get("th_rsi", ""),
        "th_adx_min": _safe_float(fields.get("th_adx_min")),
        "th_atr": fields.get("th_atr", ""),
        "th_spread_min": _safe_float(fields.get("th_spread_min")),
    }


def _entry_diag_rows(log_path: str, max_lines: int) -> list[dict[str, object]]:
    cache_path = str(ENTRY_DIAG_CACHE.get("path") or "")
    cache_mtime = ENTRY_DIAG_CACHE.get("mtime_ns")
    cache_size = ENTRY_DIAG_CACHE.get("size")
    try:
        stat = os.stat(log_path)
    except OSError:
        ENTRY_DIAG_CACHE["path"] = log_path
        ENTRY_DIAG_CACHE["mtime_ns"] = None
        ENTRY_DIAG_CACHE["size"] = None
        ENTRY_DIAG_CACHE["rows"] = []
        return []

    if cache_path == log_path and cache_mtime == stat.st_mtime_ns and cache_size == stat.st_size:
        cached_rows = ENTRY_DIAG_CACHE.get("rows", [])
        if isinstance(cached_rows, list):
            return cached_rows

    tail: deque[str] = deque(maxlen=max(1000, max_lines))
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                tail.append(line)
    except OSError:
        ENTRY_DIAG_CACHE["path"] = log_path
        ENTRY_DIAG_CACHE["mtime_ns"] = None
        ENTRY_DIAG_CACHE["size"] = None
        ENTRY_DIAG_CACHE["rows"] = []
        return []

    rows: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for line in reversed(tail):
        row = _parse_entry_diag_line(line)
        if row is None:
            continue
        key = (str(row.get("pair", "")), str(row.get("candle", "")))
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    ENTRY_DIAG_CACHE["path"] = log_path
    ENTRY_DIAG_CACHE["mtime_ns"] = stat.st_mtime_ns
    ENTRY_DIAG_CACHE["size"] = stat.st_size
    ENTRY_DIAG_CACHE["rows"] = rows
    return rows


def _filter_entry_diag_rows(
    rows: list[dict[str, object]],
    *,
    symbol: str | None,
    ts_from: str | None,
    ts_to: str | None,
) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for row in rows:
        pair = str(row.get("pair") or "").upper()
        if symbol and pair != symbol:
            continue

        ts_iso = str(row.get("ts_iso") or "")
        if ts_from and ts_iso and ts_iso < ts_from:
            continue
        if ts_to and ts_iso and ts_iso >= ts_to:
            continue
        filtered.append(row)
    return filtered


def _rotation_log_path() -> str:
    for key in ("SPIKE_ROTATION_LOG_PATH", "LLM_ROTATE_LOG_PATH"):
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    return "/data/llm-pair-rotation.log"


def _rotation_log_rows(log_path: str, max_lines: int = 400) -> list[dict[str, object]]:
    cache_path = str(ROTATION_LOG_CACHE.get("path") or "")
    cache_mtime = ROTATION_LOG_CACHE.get("mtime_ns")
    cache_size = ROTATION_LOG_CACHE.get("size")
    try:
        stat = os.stat(log_path)
    except OSError:
        ROTATION_LOG_CACHE["path"] = log_path
        ROTATION_LOG_CACHE["mtime_ns"] = None
        ROTATION_LOG_CACHE["size"] = None
        ROTATION_LOG_CACHE["rows"] = []
        return []

    if cache_path == log_path and cache_mtime == stat.st_mtime_ns and cache_size == stat.st_size:
        cached_rows = ROTATION_LOG_CACHE.get("rows", [])
        if isinstance(cached_rows, list):
            return cached_rows

    tail: deque[str] = deque(maxlen=max(20, max_lines))
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                tail.append(line)
    except OSError:
        ROTATION_LOG_CACHE["path"] = log_path
        ROTATION_LOG_CACHE["mtime_ns"] = stat.st_mtime_ns
        ROTATION_LOG_CACHE["size"] = stat.st_size
        ROTATION_LOG_CACHE["rows"] = []
        return []

    rows: list[dict[str, object]] = []
    for line in reversed(tail):
        raw = line.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        event = str(payload.get("event", "")).strip()
        if event not in {"rotation_decision", "rotation_no_candidates"}:
            continue
        rows.append(payload)

    ROTATION_LOG_CACHE["path"] = log_path
    ROTATION_LOG_CACHE["mtime_ns"] = stat.st_mtime_ns
    ROTATION_LOG_CACHE["size"] = stat.st_size
    ROTATION_LOG_CACHE["rows"] = rows
    return rows


def _latest_entry_diag_by_pair(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    latest: dict[str, dict[str, object]] = {}
    for row in rows:
        pair = str(row.get("pair") or "").upper()
        if pair and pair not in latest:
            latest[pair] = row
    return latest


def _rotation_stage_sort(stage: str) -> int:
    order = {
        "selected_for_bot": 0,
        "ranked_rejected": 1,
        "prefilter_rejected": 2,
        "metrics_rejected": 3,
        "whitelist_missing": 4,
    }
    return order.get(stage, 9)


def _rotation_pair_rows(event: dict[str, object], latest_entry_diags: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    selected_pairs = {
        str(item).strip().upper()
        for item in event.get("selected_pairs", [])
        if str(item).strip()
    }
    candidate_news = event.get("candidate_news", {})
    if not isinstance(candidate_news, dict):
        candidate_news = {}
    rows: list[dict[str, object]] = []

    decisions = event.get("decisions", [])
    if isinstance(decisions, list):
        for item in decisions:
            if not isinstance(item, dict):
                continue
            pair = str(item.get("pair", "")).strip().upper()
            if not pair:
                continue
            diag = latest_entry_diags.get(pair, {})
            news = item.get("coin_news_context", {})
            if not isinstance(news, dict):
                news = candidate_news.get(pair, {}) if isinstance(candidate_news.get(pair, {}), dict) else {}
            selected = bool(item.get("selected", pair in selected_pairs))
            stage = str(item.get("selection_status", "selected_for_bot" if selected else "ranked_rejected"))
            rows.append(
                {
                    "pair": pair,
                    "stage": stage,
                    "selected": selected,
                    "data_source": str(item.get("data_source", "")),
                    "regime": str(item.get("regime", "")),
                    "risk_level": str(item.get("risk_level", "")),
                    "confidence": item.get("confidence"),
                    "final_score": item.get("final_score"),
                    "market_rank_score": item.get("market_rank_score"),
                    "trading_signal_side": str(item.get("trading_signal_side", "")),
                    "trading_signal_score": item.get("trading_signal_score"),
                    "reason": str(item.get("selection_reason", item.get("note", ""))),
                    "news_count_24h": news.get("news_count_24h"),
                    "news_sentiment": str(news.get("sentiment", "")),
                    "news_note": str(news.get("note", "")),
                    "news_risk_flags": ", ".join(str(value) for value in news.get("risk_flags", []) if str(value).strip())
                    if isinstance(news.get("risk_flags"), list)
                    else "",
                    "news_headlines": " | ".join(str(value) for value in news.get("top_headlines", []) if str(value).strip())
                    if isinstance(news.get("top_headlines"), list)
                    else "",
                    "bot_status": str(diag.get("status", "")),
                    "bot_failed": str(diag.get("failed", "")),
                    "bot_candle": str(diag.get("candle", "")),
                }
            )

    prefilter_rejected = event.get("prefilter_rejected", [])
    if isinstance(prefilter_rejected, list):
        for item in prefilter_rejected:
            if not isinstance(item, dict):
                continue
            pair = str(item.get("pair", "")).strip().upper()
            if not pair:
                continue
            diag = latest_entry_diags.get(pair, {})
            news = candidate_news.get(pair, {}) if isinstance(candidate_news.get(pair, {}), dict) else {}
            reasons = item.get("reasons", [])
            if isinstance(reasons, list):
                reason_text = ", ".join(str(value) for value in reasons if str(value).strip())
            else:
                reason_text = str(reasons)
            rows.append(
                {
                    "pair": pair,
                    "stage": "prefilter_rejected",
                    "selected": False,
                    "data_source": "",
                    "regime": "",
                    "risk_level": "",
                    "confidence": None,
                    "final_score": None,
                    "market_rank_score": None,
                    "trading_signal_side": "",
                    "trading_signal_score": None,
                    "reason": reason_text,
                    "news_count_24h": news.get("news_count_24h"),
                    "news_sentiment": str(news.get("sentiment", "")),
                    "news_note": str(news.get("note", "")),
                    "news_risk_flags": ", ".join(str(value) for value in news.get("risk_flags", []) if str(value).strip())
                    if isinstance(news.get("risk_flags"), list)
                    else "",
                    "news_headlines": " | ".join(str(value) for value in news.get("top_headlines", []) if str(value).strip())
                    if isinstance(news.get("top_headlines"), list)
                    else "",
                    "bot_status": str(diag.get("status", "")),
                    "bot_failed": str(diag.get("failed", "")),
                    "bot_candle": str(diag.get("candle", "")),
                }
            )

    skipped = event.get("skipped", [])
    if isinstance(skipped, list):
        for item in skipped:
            if not isinstance(item, dict):
                continue
            pair = str(item.get("pair", "")).strip().upper()
            if not pair:
                continue
            diag = latest_entry_diags.get(pair, {})
            news = candidate_news.get(pair, {}) if isinstance(candidate_news.get(pair, {}), dict) else {}
            rows.append(
                {
                    "pair": pair,
                    "stage": "metrics_rejected",
                    "selected": False,
                    "data_source": "",
                    "regime": "",
                    "risk_level": "",
                    "confidence": None,
                    "final_score": None,
                    "market_rank_score": None,
                    "trading_signal_side": "",
                    "trading_signal_score": None,
                    "reason": str(item.get("reason", "")),
                    "news_count_24h": news.get("news_count_24h"),
                    "news_sentiment": str(news.get("sentiment", "")),
                    "news_note": str(news.get("note", "")),
                    "news_risk_flags": ", ".join(str(value) for value in news.get("risk_flags", []) if str(value).strip())
                    if isinstance(news.get("risk_flags"), list)
                    else "",
                    "news_headlines": " | ".join(str(value) for value in news.get("top_headlines", []) if str(value).strip())
                    if isinstance(news.get("top_headlines"), list)
                    else "",
                    "bot_status": str(diag.get("status", "")),
                    "bot_failed": str(diag.get("failed", "")),
                    "bot_candle": str(diag.get("candle", "")),
                }
            )

    whitelist_missing = event.get("whitelist_missing", [])
    if isinstance(whitelist_missing, list):
        for item in whitelist_missing:
            pair = str(item).strip().upper()
            if not pair:
                continue
            diag = latest_entry_diags.get(pair, {})
            news = candidate_news.get(pair, {}) if isinstance(candidate_news.get(pair, {}), dict) else {}
            rows.append(
                {
                    "pair": pair,
                    "stage": "whitelist_missing",
                    "selected": False,
                    "data_source": "",
                    "regime": "",
                    "risk_level": "",
                    "confidence": None,
                    "final_score": None,
                    "market_rank_score": None,
                    "trading_signal_side": "",
                    "trading_signal_score": None,
                    "reason": "not_in_pair_whitelist",
                    "news_count_24h": news.get("news_count_24h"),
                    "news_sentiment": str(news.get("sentiment", "")),
                    "news_note": str(news.get("note", "")),
                    "news_risk_flags": ", ".join(str(value) for value in news.get("risk_flags", []) if str(value).strip())
                    if isinstance(news.get("risk_flags"), list)
                    else "",
                    "news_headlines": " | ".join(str(value) for value in news.get("top_headlines", []) if str(value).strip())
                    if isinstance(news.get("top_headlines"), list)
                    else "",
                    "bot_status": str(diag.get("status", "")),
                    "bot_failed": str(diag.get("failed", "")),
                    "bot_candle": str(diag.get("candle", "")),
                }
            )

    rows.sort(key=lambda row: (_rotation_stage_sort(str(row.get("stage", ""))), -float(row.get("final_score") or -1.0), str(row.get("pair", ""))))
    return rows


def _pager_html(
    request: web.Request,
    *,
    param: str,
    page: int,
    page_size: int,
    total_rows: int,
) -> str:
    total_pages = _total_pages(total_rows, page_size)
    current = max(1, min(page, total_pages))
    start_row = ((current - 1) * page_size + 1) if total_rows else 0
    end_row = min(current * page_size, total_rows) if total_rows else 0

    parts: list[str] = []
    parts.append(
        (
            f'<span class="pager-meta">Showing {start_row}-{end_row} of {total_rows} '
            f"(page {current}/{total_pages})</span>"
        )
    )

    nav: list[str] = []
    if current > 1:
        nav.append(f'<a class="page-btn" href="{_esc(_query_href(request, {param: 1}))}">First</a>')
        nav.append(f'<a class="page-btn" href="{_esc(_query_href(request, {param: current - 1}))}">Prev</a>')
    else:
        nav.append('<span class="page-btn disabled">First</span>')
        nav.append('<span class="page-btn disabled">Prev</span>')

    window_start = max(1, current - 2)
    window_end = min(total_pages, current + 2)
    for page_num in range(window_start, window_end + 1):
        if page_num == current:
            nav.append(f'<span class="page-btn current">{page_num}</span>')
        else:
            nav.append(f'<a class="page-btn" href="{_esc(_query_href(request, {param: page_num}))}">{page_num}</a>')

    if current < total_pages:
        nav.append(f'<a class="page-btn" href="{_esc(_query_href(request, {param: current + 1}))}">Next</a>')
        nav.append(f'<a class="page-btn" href="{_esc(_query_href(request, {param: total_pages}))}">Last</a>')
    else:
        nav.append('<span class="page-btn disabled">Next</span>')
        nav.append('<span class="page-btn disabled">Last</span>')

    jump_form = (
        f'<form class="jump-form" method="get">'
        f"{_hidden_query_inputs(request, {param})}"
        f'<label>Go to</label>'
        f'<input type="number" min="1" max="{total_pages}" name="{_esc(param)}" value="{current}" />'
        f'<button type="submit">Go</button>'
        f"</form>"
    )

    parts.append(f'<div class="pager-nav">{"".join(nav)}{jump_form}</div>')
    return "".join(parts)


def _page_size_controls_html(request: web.Request, page_size: int) -> str:
    options = [20, 40, 80, 120, 200]
    option_tags: list[str] = []
    for option in options:
        selected = ' selected="selected"' if option == page_size else ""
        option_tags.append(f'<option value="{option}"{selected}>{option}</option>')

    hidden = _hidden_query_inputs(
        request,
        {"page_size", "alerts_page", "outcomes_page", "evals_page", "diag_page", "debug_page"},
    )
    return (
        '<form class="page-size-form" method="get">'
        f"{hidden}"
        '<label for="page_size">Rows per table</label>'
        '<select id="page_size" name="page_size" onchange="this.form.submit()">'
        f"{''.join(option_tags)}"
        "</select>"
        '<noscript><button type="submit">Apply</button></noscript>'
        "</form>"
    )


def _filter_controls_html(
    request: web.Request,
    *,
    symbol: str,
    llm_verdict: str,
    outcomes_status: str,
    date_from: str,
    date_to: str,
) -> str:
    hidden = _hidden_query_inputs(
        request,
        {
            "symbol",
            "llm_verdict",
            "outcomes_status",
            "date_from",
            "date_to",
            "alerts_page",
            "outcomes_page",
            "evals_page",
            "diag_page",
            "debug_page",
        },
    )
    allowed_selected = ' selected="selected"' if llm_verdict == "allowed" else ""
    blocked_selected = ' selected="selected"' if llm_verdict == "blocked" else ""
    unknown_selected = ' selected="selected"' if llm_verdict == "unknown" else ""
    all_selected = ' selected="selected"' if llm_verdict == "all" else ""
    pending_selected = ' selected="selected"' if outcomes_status == "pending" else ""
    resolved_selected = ' selected="selected"' if outcomes_status == "resolved" else ""
    status_all_selected = ' selected="selected"' if outcomes_status == "all" else ""
    clear_href = _query_href(
        request,
        {},
        drop={
            "symbol",
            "llm_verdict",
            "outcomes_status",
            "date_from",
            "date_to",
            "alerts_page",
            "outcomes_page",
            "evals_page",
            "diag_page",
            "debug_page",
        },
    )

    return (
        '<form class="filter-form" method="get">'
        f"{hidden}"
        '<label for="symbol">Symbol</label>'
        f'<input id="symbol" name="symbol" placeholder="BTC/USDT" value="{_esc(symbol)}" />'
        '<label for="llm_verdict">LLM</label>'
        '<select id="llm_verdict" name="llm_verdict">'
        f'<option value="all"{all_selected}>All</option>'
        f'<option value="allowed"{allowed_selected}>Allowed</option>'
        f'<option value="blocked"{blocked_selected}>Blocked</option>'
        f'<option value="unknown"{unknown_selected}>Unknown</option>'
        "</select>"
        '<label for="outcomes_status">Outcome</label>'
        '<select id="outcomes_status" name="outcomes_status">'
        f'<option value="all"{status_all_selected}>All</option>'
        f'<option value="pending"{pending_selected}>Pending</option>'
        f'<option value="resolved"{resolved_selected}>Resolved</option>'
        "</select>"
        '<label for="date_from">From</label>'
        f'<input id="date_from" type="date" name="date_from" value="{_esc(date_from)}" />'
        '<label for="date_to">To</label>'
        f'<input id="date_to" type="date" name="date_to" value="{_esc(date_to)}" />'
        '<button type="submit">Apply</button>'
        f'<a class="clear-link" href="{_esc(clear_href)}">Clear</a>'
        "</form>"
    )


async def _fetch_llm_debug_rows(limit: int) -> tuple[list[dict], str | None]:
    if not _env_bool("SPIKE_LLM_DEBUG_TAB_ENABLED", True):
        return [], None

    bot_api_url = os.getenv("SPIKE_LLM_DEBUG_BOT_API_URL", "").strip() or _shared_bot_api_url()
    try:
        timeout_raw = float(os.getenv("SPIKE_LLM_DEBUG_TIMEOUT_SECONDS", "3"))
    except ValueError:
        timeout_raw = 3.0
    timeout_seconds = max(1.0, min(15.0, timeout_raw))
    url = f"{bot_api_url.rstrip('/')}/debug/llm-calls?limit={int(limit)}"

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return [], f"bot-api debug endpoint status={response.status}"
                payload = await response.json()
    except Exception as exc:
        return [], f"bot-api debug endpoint error: {str(exc)[:120]}"

    items = payload.get("items")
    if not isinstance(items, list):
        return [], "bot-api debug endpoint returned invalid payload"
    normalized: list[dict] = [item for item in items if isinstance(item, dict)]
    return normalized, None


async def dashboard(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    page_size = _parse_int(request.query.get("page_size"), 40, 10, 200)
    alerts_page_requested = _parse_int(request.query.get("alerts_page"), 1, 1, 100000)
    outcomes_page_requested = _parse_int(request.query.get("outcomes_page"), 1, 1, 100000)
    evals_page_requested = _parse_int(request.query.get("evals_page"), 1, 1, 100000)
    diag_page_requested = _parse_int(request.query.get("diag_page"), 1, 1, 100000)
    debug_page_requested = _parse_int(request.query.get("debug_page"), 1, 1, 100000)
    symbol_filter = _normalize_symbol(request.query.get("symbol"))
    llm_verdict_raw = str(request.query.get("llm_verdict", "all")).strip().lower()
    llm_verdict = llm_verdict_raw if llm_verdict_raw in {"all", "allowed", "blocked", "unknown"} else "all"
    llm_allowed_filter = _parse_llm_verdict(llm_verdict)
    outcomes_status_raw = str(request.query.get("outcomes_status", "all")).strip().lower()
    outcomes_status = outcomes_status_raw if outcomes_status_raw in {"all", "pending", "resolved"} else "all"
    outcomes_status_filter = _parse_outcomes_status(outcomes_status)
    date_from = _parse_date(request.query.get("date_from"))
    date_to = _parse_date(request.query.get("date_to"))
    ts_from, ts_to = _date_bounds(date_from, date_to)

    alerts_total = store.count_alerts(
        symbol=symbol_filter,
        llm_allowed=llm_allowed_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    outcomes_total = store.count_outcomes(
        status=outcomes_status_filter,
        symbol=symbol_filter,
        llm_allowed=llm_allowed_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    evals_total = store.count_llm_shadow_evals(
        symbol=symbol_filter,
        llm_allowed=llm_allowed_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    entry_diag_log_path = os.getenv("FREQTRADE_LOG_PATH", "/data/freqtrade.log")
    entry_diag_max_lines = _parse_int(os.getenv("FREQTRADE_DIAG_MAX_LINES"), 200000, 1000, 2000000)
    entry_diag_rows = _entry_diag_rows(entry_diag_log_path, entry_diag_max_lines)
    rotation_log_path = _rotation_log_path()
    rotation_events = _rotation_log_rows(rotation_log_path)
    latest_rotation_event = rotation_events[0] if rotation_events else {}
    latest_rotation_pairs = _rotation_pair_rows(latest_rotation_event, _latest_entry_diag_by_pair(entry_diag_rows)) if latest_rotation_event else []
    coin_news_rows = _load_coin_news_rows(limit=20)
    if symbol_filter:
        latest_rotation_pairs = [row for row in latest_rotation_pairs if str(row.get("pair", "")).upper() == symbol_filter]
    filtered_entry_diags = _filter_entry_diag_rows(
        entry_diag_rows,
        symbol=symbol_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    diag_total = len(filtered_entry_diags)
    debug_fetch_limit = _parse_int(os.getenv("SPIKE_LLM_DEBUG_FETCH_LIMIT"), 500, 20, 5000)
    llm_debug_rows, llm_debug_error = await _fetch_llm_debug_rows(debug_fetch_limit)
    debug_total = len(llm_debug_rows)
    daily_guard = await _daily_guard_snapshot()
    bot_perf_24h = _load_bot_performance_snapshot(24)
    bot_perf_7d = _load_bot_performance_snapshot(24 * 7)

    alerts_page = min(alerts_page_requested, _total_pages(alerts_total, page_size))
    outcomes_page = min(outcomes_page_requested, _total_pages(outcomes_total, page_size))
    evals_page = min(evals_page_requested, _total_pages(evals_total, page_size))
    diag_page = min(diag_page_requested, _total_pages(diag_total, page_size))
    debug_page = min(debug_page_requested, _total_pages(debug_total, page_size))

    alerts_offset = (alerts_page - 1) * page_size
    outcomes_offset = (outcomes_page - 1) * page_size
    evals_offset = (evals_page - 1) * page_size
    diag_offset = (diag_page - 1) * page_size
    debug_offset = (debug_page - 1) * page_size

    alerts = store.fetch_recent_alerts(
        limit=page_size,
        offset=alerts_offset,
        symbol=symbol_filter,
        llm_allowed=llm_allowed_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    outcomes = store.fetch_recent_outcomes(
        limit=page_size,
        offset=outcomes_offset,
        status=outcomes_status_filter,
        symbol=symbol_filter,
        llm_allowed=llm_allowed_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    llm_evals = store.fetch_recent_llm_shadow_evals(
        limit=page_size,
        offset=evals_offset,
        symbol=symbol_filter,
        llm_allowed=llm_allowed_filter,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    entry_diags = filtered_entry_diags[diag_offset : diag_offset + page_size]
    llm_debug_page_rows = llm_debug_rows[debug_offset : debug_offset + page_size]

    summary = store.fetch_horizon_summary()
    llm_summary = store.fetch_llm_outcome_summary()

    summary_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row.get('horizon_minutes', 0))}</td>"
                f"<td>{int(row.get('total', 0))}</td>"
                f"<td>{int(row.get('resolved', 0))}</td>"
                f"<td>{_fmt_pct(row.get('avg_return_pct'))}</td>"
                f"<td>{_fmt_pct((row.get('win_rate') or 0.0) * 100.0)}</td></tr>"
            )
            for row in summary
        ]
    )

    alert_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row['id'])}</td>"
                f"<td>{_utc_time_html(row.get('ts'))}</td>"
                f"<td>{_esc(row.get('symbol'))}</td>"
                f"<td>{_fmt_num(row.get('score'), 4)}</td>"
                f"<td>{_fmt_num(row.get('entry_price'), 8)}</td>"
                f"<td>{_esc(_fmt_llm_allowed(row.get('llm_allowed')))}</td>"
                f"<td>{_esc(row.get('llm_regime'))}</td>"
                f"<td>{_esc(row.get('llm_risk_level'))}</td>"
                f"<td>{_fmt_num(row.get('llm_confidence'), 4)}</td>"
                f"<td>{_esc(row.get('llm_reason'))}</td></tr>"
            )
            for row in alerts
        ]
    )

    outcome_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row['id'])}</td>"
                f"<td>{int(row.get('alert_id', 0))}</td>"
                f"<td>{_esc(row.get('symbol'))}</td>"
                f"<td>{int(row.get('horizon_minutes', 0))}</td>"
                f"<td>{_esc(row.get('status'))}</td>"
                f"<td>{_fmt_num(row.get('entry_price'), 8)}</td>"
                f"<td>{_fmt_num(row.get('observed_price'), 8)}</td>"
                f"<td>{_fmt_pct(row.get('return_pct'))}</td>"
                f"<td>{_esc(_fmt_llm_allowed(row.get('llm_allowed')))}</td>"
                f"<td>{_fmt_num(row.get('llm_confidence'), 4)}</td>"
                f"<td>{_utc_time_html(row.get('due_ts'))}</td>"
                f"<td>{_utc_time_html(row.get('resolved_ts'))}</td></tr>"
            )
            for row in outcomes
        ]
    )

    llm_summary_rows = "\n".join(
        [
            (
                f"<tr><td>{_fmt_llm_allowed(row.get('llm_allowed'))}</td>"
                f"<td>{int(row.get('horizon_minutes', 0))}</td>"
                f"<td>{int(row.get('resolved', 0))}</td>"
                f"<td>{_fmt_pct(row.get('avg_return_pct'))}</td>"
                f"<td>{_fmt_pct((row.get('win_rate') or 0.0) * 100.0)}</td></tr>"
            )
            for row in llm_summary
        ]
    )

    llm_eval_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row.get('id', 0))}</td>"
                f"<td>{_utc_time_html(row.get('ts'))}</td>"
                f"<td>{_esc(row.get('symbol'))}</td>"
                f"<td>{_fmt_num(row.get('score'), 4)}</td>"
                f"<td>{_fmt_num(row.get('spread_pct'), 4)}</td>"
                f"<td>{'yes' if int(row.get('threshold_ok', 0)) == 1 else 'no'}</td>"
                f"<td>{'yes' if int(row.get('cooldown_ok', 0)) == 1 else 'no'}</td>"
                f"<td>{'yes' if int(row.get('eligible_alert', 0)) == 1 else 'no'}</td>"
                f"<td>{_esc(_fmt_llm_allowed(row.get('llm_allowed')))}</td>"
                f"<td>{_esc(row.get('llm_regime'))}</td>"
                f"<td>{_esc(row.get('llm_risk_level'))}</td>"
                f"<td>{_fmt_num(row.get('llm_confidence'), 4)}</td>"
                f"<td>{_esc(row.get('llm_reason'))}</td>"
                f"<td>{_esc(row.get('llm_latency_ms'))}</td></tr>"
            )
            for row in llm_evals
        ]
    )

    entry_diag_rows_html = "\n".join(
        [
            (
                f"<tr><td>{_utc_time_html(row.get('ts_iso'), row.get('ts'))}</td>"
                f"<td>{_esc(row.get('pair'))}</td>"
                f"<td>{_utc_time_html(row.get('candle'))}</td>"
                f"<td>{_esc(row.get('status'))}</td>"
                f"<td>{'yes' if int(row.get('base_ok') or 0) == 1 else 'no'}</td>"
                f"<td>{'yes' if int(row.get('final_ok') or 0) == 1 else 'no'}</td>"
                f"<td>{_esc(row.get('tag'))}</td>"
                f"<td>{_esc(row.get('failed'))}</td>"
                f"<td>{_fmt_num(row.get('rsi'), 2)}</td>"
                f"<td>{_fmt_num(row.get('adx'), 2)}</td>"
                f"<td>{_fmt_num(row.get('atr_pct'), 2)}</td>"
                f"<td>{_fmt_num(row.get('spread'), 3)}</td>"
                f"<td>{_fmt_num(row.get('vol_z'), 2)}</td>"
                f"<td>{'yes' if int(row.get('bench_risk_ok') or 0) == 1 else 'no'}</td>"
                f"<td>{_esc(row.get('th_rsi'))}</td>"
                f"<td>{_fmt_num(row.get('th_adx_min'), 2)}</td>"
                f"<td>{_esc(row.get('th_atr'))}</td>"
                f"<td>{_fmt_num(row.get('th_spread_min'), 3)}</td></tr>"
            )
            for row in entry_diags
        ]
    )
    llm_debug_rows_html = "\n".join(
        [
            (
                f"<tr><td>{_utc_time_html(row.get('ts'))}</td>"
                f"<td>{_esc(row.get('endpoint'))}</td>"
                f"<td>{_esc(row.get('model'))}</td>"
                f"<td>{'yes' if bool(row.get('parsed_ok')) else 'no'}</td>"
                f"<td>{_esc(row.get('error'))}</td>"
                f"<td class=\"code-cell\">{_code_cell_html(row.get('prompt'))}</td>"
                f"<td class=\"code-cell\">{_code_cell_html(row.get('response'))}</td></tr>"
            )
            for row in llm_debug_page_rows
        ]
    )
    rotation_runs_rows_html = "\n".join(
        [
            (
                f"<tr><td>{_utc_time_html(row.get('timestamp'))}</td>"
                f"<td>{_esc(row.get('event'))}</td>"
                f"<td>{_esc(row.get('source'))}</td>"
                f"<td>{_esc(row.get('reason'))}</td>"
                f"<td>{int(row.get('candidate_count', 0) or 0)}</td>"
                f"<td>{int(row.get('selected_count', len(row.get('selected_pairs', []) if isinstance(row.get('selected_pairs'), list) else [])) or 0)}</td>"
                f"<td>{int(row.get('prefilter_rejected_count', 0) or 0)}</td>"
                f"<td>{_fmt_num(row.get('avg_selected_confidence'), 3)}</td>"
                f"<td>{_fmt_num(row.get('avg_selected_final_score'), 3)}</td>"
                f"<td>{_esc(_clip_text(' '.join([str(x) for x in row.get('selected_pairs', [])]), 80))}</td></tr>"
            )
            for row in rotation_events[:12]
            if isinstance(row, dict)
        ]
    )
    rotation_pairs_rows_html = "\n".join(
        [
            (
                f"<tr><td>{_esc(row.get('pair'))}</td>"
                f"<td>{_esc(row.get('stage'))}</td>"
                f"<td>{'yes' if bool(row.get('selected')) else 'no'}</td>"
                f"<td>{_esc(row.get('data_source'))}</td>"
                f"<td>{_esc(row.get('regime'))}</td>"
                f"<td>{_esc(row.get('risk_level'))}</td>"
                f"<td>{_fmt_num(row.get('confidence'), 3)}</td>"
                f"<td>{_fmt_num(row.get('final_score'), 3)}</td>"
                f"<td>{_fmt_num(row.get('market_rank_score'), 3)}</td>"
                f"<td>{_esc(str(row.get('trading_signal_side') or ''))} {_fmt_num(row.get('trading_signal_score'), 2)}</td>"
                f"<td>{_esc(_clip_text(row.get('reason'), 120))}</td>"
                f"<td>{_esc(_rotation_news_summary(row))}</td>"
                f"<td>{_esc(_clip_text(row.get('news_note'), 120))}</td>"
                f"<td>{_esc(_clip_text(row.get('news_headlines'), 140))}</td>"
                f"<td>{_esc(row.get('bot_status'))}</td>"
                f"<td>{_utc_time_html(row.get('bot_candle'))}</td>"
                f"<td>{_esc(_clip_text(row.get('bot_failed'), 120))}</td></tr>"
            )
            for row in latest_rotation_pairs
        ]
    )
    latest_rotation_selected_pairs = latest_rotation_event.get("selected_pairs", []) if isinstance(latest_rotation_event, dict) else []
    latest_rotation_summary = (
        "Latest run: "
        f"ts={_utc_time_html(latest_rotation_event.get('timestamp')) if latest_rotation_event else 'n/a'} | "
        f"source={_esc(latest_rotation_event.get('source')) if latest_rotation_event else 'n/a'} | "
        f"reason={_esc(latest_rotation_event.get('reason')) if latest_rotation_event else 'n/a'} | "
        f"candidates={int(latest_rotation_event.get('candidate_count', 0) or 0) if latest_rotation_event else 0} | "
        f"selected={int(latest_rotation_event.get('selected_count', len(latest_rotation_selected_pairs)) or 0) if latest_rotation_event else 0} | "
        f"prefilter_rejected={int(latest_rotation_event.get('prefilter_rejected_count', 0) or 0) if latest_rotation_event else 0}"
    )
    coin_news_rows_html = "\n".join(
        [
            (
                f"<tr><td>{_esc(row.get('pair'))}</td>"
                f"<td>{_utc_time_html(row.get('updated_ts'))}</td>"
                f"<td>{int(row.get('news_count_24h', 0) or 0)}</td>"
                f"<td>{_esc(row.get('sentiment'))}</td>"
                f"<td>{'yes' if bool(row.get('major_catalyst')) else 'no'}</td>"
                f"<td>{_esc(_clip_text(row.get('risk_flags'), 80))}</td>"
                f"<td>{_esc(_clip_text(row.get('note'), 120))}</td>"
                f"<td>{_esc(_clip_text(row.get('headlines'), 180))}</td></tr>"
            )
            for row in coin_news_rows
        ]
    )

    alerts_pager = _pager_html(
        request,
        param="alerts_page",
        page=alerts_page,
        page_size=page_size,
        total_rows=alerts_total,
    )
    outcomes_pager = _pager_html(
        request,
        param="outcomes_page",
        page=outcomes_page,
        page_size=page_size,
        total_rows=outcomes_total,
    )
    evals_pager = _pager_html(
        request,
        param="evals_page",
        page=evals_page,
        page_size=page_size,
        total_rows=evals_total,
    )
    diag_pager = _pager_html(
        request,
        param="diag_page",
        page=diag_page,
        page_size=page_size,
        total_rows=diag_total,
    )
    debug_pager = _pager_html(
        request,
        param="debug_page",
        page=debug_page,
        page_size=page_size,
        total_rows=debug_total,
    )
    page_size_controls = _page_size_controls_html(request, page_size)
    filter_controls = _filter_controls_html(
        request,
        symbol=symbol_filter or "",
        llm_verdict=llm_verdict,
        outcomes_status=outcomes_status,
        date_from=date_from,
        date_to=date_to,
    )
    active_filters: list[str] = []
    if symbol_filter:
        active_filters.append(f"symbol={symbol_filter}")
    if llm_verdict != "all":
        active_filters.append(f"llm={llm_verdict}")
    if outcomes_status != "all":
        active_filters.append(f"outcomes={outcomes_status}")
    if date_from:
        active_filters.append(f"from={date_from}")
    if date_to:
        active_filters.append(f"to={date_to}")
    filter_summary = " | ".join(active_filters) if active_filters else "none"
    llm_debug_error_html = (
        f'<div class="muted warn">LLM debug feed issue: {_esc(llm_debug_error)}</div>' if llm_debug_error else ""
    )
    daily_guard_error = " | ".join(
        [part for part in [str(daily_guard.get("api_error") or ""), str(daily_guard.get("db_error") or "")] if part]
    )
    daily_guard_error_html = (
        f'<div class="muted warn">Daily guard data issue: {_esc(daily_guard_error)}</div>' if daily_guard_error else ""
    )
    daily_guard_cards_html = "".join(
        [
            (
                f'<div class="metric-card"><div class="metric-label">Entries Allowed</div>'
                f'<div class="metric-value">{_esc("yes" if bool(daily_guard.get("allow_entries")) else "no")}</div>'
                f'<div class="metric-sub">{_esc(str(daily_guard.get("reason") or ""))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">Daily Realized</div>'
                f'<div class="metric-value">{_fmt_pct(daily_guard.get("realized_pct"))}</div>'
                f'<div class="metric-sub">{_esc(_fmt_money(daily_guard.get("realized_abs")))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">Daily Target</div>'
                f'<div class="metric-value">{_fmt_pct(daily_guard.get("target_pct"))}</div>'
                f'<div class="metric-sub">{_esc(_fmt_money(daily_guard.get("target_abs")))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">Daily Max Loss</div>'
                f'<div class="metric-value">{_fmt_pct(daily_guard.get("max_loss_pct"))}</div>'
                f'<div class="metric-sub">{_esc(_fmt_money(daily_guard.get("max_loss_abs")))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">PnL Basis</div>'
                f'<div class="metric-value">{_esc(str(daily_guard.get("mode") or ""))}</div>'
                f'<div class="metric-sub">{_esc(_fmt_money(daily_guard.get("base_capital")))} via {_esc(str(daily_guard.get("base_source") or ""))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">Live Equity</div>'
                f'<div class="metric-value">{_esc(_fmt_money(daily_guard.get("total_bot") or daily_guard.get("total_balance")))}</div>'
                f'<div class="metric-sub">open={_esc(str(daily_guard.get("open_trades") if daily_guard.get("open_trades") is not None else "n/a"))} '
                f'closed_today={_esc(str(daily_guard.get("closed_trades_today") or 0))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">Stake Sizing</div>'
                f'<div class="metric-value">{_fmt_num(daily_guard.get("stake_total_balance_pct"), 2)}%</div>'
                f'<div class="metric-sub">of total stake capital per new trade</div></div>'
            ),
        ]
    )
    bot_perf_error = " | ".join(
        [part for part in [str(bot_perf_24h.get("db_error") or ""), str(bot_perf_7d.get("db_error") or "")] if part]
    )
    bot_perf_error_html = (
        f'<div class="muted warn">Bot performance data issue: {_esc(bot_perf_error)}</div>' if bot_perf_error else ""
    )
    bot_perf_cards_html = "".join(
        [
            (
                f'<div class="metric-card"><div class="metric-label">Bot 24h</div>'
                f'<div class="metric-value">{_esc(_fmt_money(bot_perf_24h.get("net_profit_abs")))}</div>'
                f'<div class="metric-sub">closed={int(bot_perf_24h.get("closed_trades") or 0)} '
                f'win={_fmt_pct(bot_perf_24h.get("win_rate_pct"))} avg={_fmt_pct(bot_perf_24h.get("avg_profit_pct"))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">Bot 7d</div>'
                f'<div class="metric-value">{_esc(_fmt_money(bot_perf_7d.get("net_profit_abs")))}</div>'
                f'<div class="metric-sub">closed={int(bot_perf_7d.get("closed_trades") or 0)} '
                f'win={_fmt_pct(bot_perf_7d.get("win_rate_pct"))} avg={_fmt_pct(bot_perf_7d.get("avg_profit_pct"))}</div></div>'
            ),
            (
                f'<div class="metric-card"><div class="metric-label">PnL Concentration 7d</div>'
                f'<div class="metric-value">{_fmt_pct(bot_perf_7d.get("concentration_pct"))}</div>'
                f'<div class="metric-sub">best={_esc(str(bot_perf_7d.get("best_pair") or "n/a"))} '
                f'{_esc(_fmt_money(bot_perf_7d.get("best_trade_abs")))} / worst {_esc(_fmt_money(bot_perf_7d.get("worst_trade_abs")))}</div></div>'
            ),
        ]
    )

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Spike Scanner Dashboard</title>
  <style>
    :root {{
      --bg: #f3f6f9;
      --card: #ffffff;
      --border: #d8e1e8;
      --text: #1a2433;
      --muted: #5b6878;
      --accent: #1268c7;
      --accent-soft: #e9f2ff;
    }}
    body {{ font-family: ui-sans-serif, -apple-system, Segoe UI, sans-serif; margin: 20px; color: var(--text); background: radial-gradient(circle at top right, #eef8ff, var(--bg)); }}
    h1 {{ margin: 0 0 12px; }}
    h2 {{ margin-top: 28px; margin-bottom: 6px; }}
    .toolbar, .section {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; box-shadow: 0 1px 2px rgba(9, 30, 66, 0.06); }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; }}
    .metric-card {{ border: 1px solid #dde6ef; border-radius: 10px; padding: 12px; background: linear-gradient(180deg, #fbfdff, #f5f9fd); }}
    .metric-label {{ font-size: 12px; color: var(--muted); margin-bottom: 6px; }}
    .metric-value {{ font-size: 24px; font-weight: 700; line-height: 1.1; }}
    .metric-sub {{ font-size: 12px; color: var(--muted); margin-top: 6px; white-space: normal; }}
    .toolbar {{ display: flex; flex-direction: column; gap: 12px; }}
    .toolbar-top {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; justify-content: space-between; }}
    .toolbar-forms {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 8px; font-size: 13px; }}
    th, td {{ border: 1px solid #dde6ef; padding: 6px 8px; text-align: left; white-space: nowrap; }}
    th {{ background: #f7fbff; position: sticky; top: 0; z-index: 1; }}
    .wrap {{ overflow: auto; max-height: 420px; border: 1px solid #ecf2f8; border-radius: 8px; background: #fff; }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .pager {{ margin-top: 10px; display: flex; flex-direction: column; gap: 8px; }}
    .pager-meta {{ font-size: 12px; color: var(--muted); }}
    .pager-nav {{ display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }}
    .page-btn {{ display: inline-flex; align-items: center; justify-content: center; min-width: 34px; height: 28px; padding: 0 10px; border: 1px solid #cdd8e5; border-radius: 6px; background: #fff; color: #1e2e42; text-decoration: none; font-size: 12px; }}
    .page-btn:hover {{ border-color: var(--accent); color: var(--accent); }}
    .page-btn.current {{ background: var(--accent-soft); border-color: var(--accent); color: var(--accent); font-weight: 600; }}
    .page-btn.disabled {{ color: #9aa8b7; border-color: #e2eaf2; background: #f6f9fc; }}
    .jump-form {{ display: inline-flex; align-items: center; gap: 6px; margin-left: 10px; }}
    .jump-form input, .page-size-form select {{ width: 84px; height: 28px; border: 1px solid #cdd8e5; border-radius: 6px; padding: 2px 6px; font-size: 12px; }}
    .jump-form button, .page-size-form button {{ height: 28px; border: 1px solid #cdd8e5; border-radius: 6px; background: #fff; padding: 0 10px; cursor: pointer; font-size: 12px; }}
    .jump-form button:hover, .page-size-form button:hover {{ border-color: var(--accent); color: var(--accent); }}
    .page-size-form {{ display: inline-flex; align-items: center; gap: 8px; }}
    .filter-form {{ display: inline-flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
    .filter-form input, .filter-form select {{ height: 28px; border: 1px solid #cdd8e5; border-radius: 6px; padding: 2px 8px; font-size: 12px; }}
    .filter-form input[type="date"] {{ min-width: 138px; }}
    .filter-form button {{ height: 28px; border: 1px solid #cdd8e5; border-radius: 6px; background: #fff; padding: 0 10px; cursor: pointer; font-size: 12px; }}
    .filter-form button:hover {{ border-color: var(--accent); color: var(--accent); }}
    .clear-link {{ text-decoration: none; font-size: 12px; color: #35516f; border: 1px solid #d1deeb; border-radius: 6px; padding: 5px 10px; background: #f7fbff; }}
    .clear-link:hover {{ border-color: var(--accent); color: var(--accent); }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 10px; }}
    .tab-btn {{
      text-decoration: none;
      font-size: 12px;
      color: #24415f;
      border: 1px solid #d1deeb;
      border-radius: 999px;
      padding: 6px 12px;
      background: #f7fbff;
      cursor: pointer;
    }}
    .tab-btn.active {{ color: #fff; border-color: var(--accent); background: var(--accent); }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .code-cell {{
      white-space: normal;
      min-width: 0;
      max-width: none;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 11px;
      line-height: 1.35;
    }}
    .code-scroll {{
      max-height: 240px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      overflow-wrap: anywhere;
      background: #f8fbff;
      border: 1px solid #e4edf6;
      border-radius: 6px;
      padding: 6px 8px;
    }}
    .llm-debug-table {{
      table-layout: fixed;
    }}
    .llm-debug-table th, .llm-debug-table td {{
      vertical-align: top;
      overflow: hidden;
    }}
    .llm-debug-table th,
    .llm-debug-table td {{
      white-space: normal;
    }}
    .llm-debug-table th:nth-child(1), .llm-debug-table td:nth-child(1) {{ width: 140px; }}
    .llm-debug-table th:nth-child(2), .llm-debug-table td:nth-child(2) {{ width: 90px; }}
    .llm-debug-table th:nth-child(3), .llm-debug-table td:nth-child(3) {{ width: 90px; }}
    .llm-debug-table th:nth-child(4), .llm-debug-table td:nth-child(4) {{ width: 70px; }}
    .llm-debug-table th:nth-child(5), .llm-debug-table td:nth-child(5) {{ width: 140px; }}
    .llm-debug-table th:nth-child(6), .llm-debug-table td:nth-child(6) {{ width: 220px; }}
    .llm-debug-table th:nth-child(7), .llm-debug-table td:nth-child(7) {{ width: 220px; }}
    .llm-debug-table td.code-cell {{
      width: 220px;
      max-width: 220px;
      min-width: 0;
      box-sizing: border-box;
    }}
    .code-details {{
      margin: 0;
      min-width: 0;
      max-width: 100%;
    }}
    .code-summary {{
      cursor: pointer;
      white-space: normal;
      overflow: hidden;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      line-clamp: 2;
      max-width: 100%;
      word-break: break-word;
      overflow-wrap: anywhere;
      color: #35516f;
      font-size: 11px;
      line-height: 1.3;
    }}
    .code-details .code-scroll {{
      display: none;
    }}
    .code-details[open] .code-scroll {{
      display: block;
      margin-top: 6px;
    }}
    .warn {{ color: #a94412; }}
    @media (max-width: 900px) {{
      .jump-form {{ margin-left: 0; }}
      .wrap {{ max-height: 360px; }}
      .toolbar-top {{ flex-direction: column; align-items: flex-start; }}
      .code-scroll {{ max-height: 180px; }}
      .llm-debug-table th:nth-child(1), .llm-debug-table td:nth-child(1) {{ width: 120px; }}
      .llm-debug-table th:nth-child(2), .llm-debug-table td:nth-child(2) {{ width: 80px; }}
      .llm-debug-table th:nth-child(3), .llm-debug-table td:nth-child(3) {{ width: 80px; }}
      .llm-debug-table th:nth-child(4), .llm-debug-table td:nth-child(4) {{ width: 60px; }}
      .llm-debug-table th:nth-child(5), .llm-debug-table td:nth-child(5) {{ width: 120px; }}
      .llm-debug-table th:nth-child(6), .llm-debug-table td:nth-child(6) {{ width: 180px; }}
      .llm-debug-table th:nth-child(7), .llm-debug-table td:nth-child(7) {{ width: 180px; }}
      .llm-debug-table td.code-cell {{ width: 180px; max-width: 180px; }}
    }}
  </style>
</head>
<body>
  <h1>Spike Scanner Dashboard</h1>
  <div class="muted">Predictions and realized outcomes (1h/4h/24h/48h by default).</div>
  <div class="muted">Display timezone: <span id="client-timezone">detecting...</span> | Stored timestamps: UTC</div>
  <div class="toolbar">
    <div class="toolbar-top">
      <div class="toolbar-forms">
        {page_size_controls}
        {filter_controls}
      </div>
    </div>
    <div class="muted">Active filters: { _esc(filter_summary) }</div>
  </div>
  <div class="tabs" role="tablist" aria-label="Dashboard sections">
    <button type="button" class="tab-btn active" data-tab="tab-overview">Overview</button>
    <button type="button" class="tab-btn" data-tab="tab-rotation">Rotation</button>
    <button type="button" class="tab-btn" data-tab="tab-predictions">Predictions</button>
    <button type="button" class="tab-btn" data-tab="tab-evals">LLM Evals</button>
    <button type="button" class="tab-btn" data-tab="tab-diags">Entry Diags</button>
    <button type="button" class="tab-btn" data-tab="tab-outcomes">Outcomes</button>
    <button type="button" class="tab-btn" data-tab="tab-debug">LLM Debug</button>
  </div>

  <section id="tab-overview" class="tab-panel active">
    <h2>Live Daily Guard</h2>
    <div class="section">
      <div class="metrics">{daily_guard_cards_html}</div>
      {daily_guard_error_html}
    </div>

    <h2>Bot Performance</h2>
    <div class="section">
      <div class="metrics">{bot_perf_cards_html}</div>
      {bot_perf_error_html}
    </div>

    <h2>Summary By Horizon</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead><tr><th>Horizon (min)</th><th>Total</th><th>Resolved</th><th>Avg Return</th><th>Win Rate</th></tr></thead>
          <tbody>{summary_rows}</tbody>
        </table>
      </div>
    </div>

    <h2>LLM Shadow Outcomes</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead><tr><th>LLM Verdict</th><th>Horizon (min)</th><th>Resolved</th><th>Avg Return</th><th>Win Rate</th></tr></thead>
          <tbody>{llm_summary_rows}</tbody>
        </table>
      </div>
    </div>
  </section>

  <section id="tab-rotation" class="tab-panel">
    <h2>Pair Rotation Funnel</h2>
    <div class="section">
      <div class="muted">Source: {_esc(rotation_log_path)}</div>
      <div class="muted">{latest_rotation_summary}</div>
      <div class="muted">Selected pairs: {_esc(" ".join([str(x) for x in latest_rotation_selected_pairs])) if latest_rotation_selected_pairs else "none"}</div>
    </div>

    <h2>Latest Rotation Pair Outcomes</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th>Pair</th><th>Stage</th><th>Selected</th><th>Source</th><th>Regime</th><th>Risk</th><th>Conf</th><th>Final</th>
              <th>Mkt Rank</th><th>Signal</th><th>Reason</th><th>News</th><th>News Note</th><th>Headlines</th><th>Bot Status</th><th>Bot Candle</th><th>Bot Failed Checks</th>
            </tr>
          </thead>
          <tbody>{rotation_pairs_rows_html}</tbody>
        </table>
      </div>
    </div>

    <h2>Coin News Cache</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th>Pair</th><th>Updated</th><th>Count 24h</th><th>Sentiment</th><th>Catalyst</th><th>Risk Flags</th><th>Note</th><th>Headlines</th>
            </tr>
          </thead>
          <tbody>{coin_news_rows_html}</tbody>
        </table>
      </div>
    </div>

    <h2>Recent Rotation Runs</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th>Timestamp</th><th>Event</th><th>Source</th><th>Reason</th><th>Candidates</th><th>Selected</th><th>Prefilter Rejected</th>
              <th>Avg Selected Conf</th><th>Avg Selected Final</th><th>Selected Pairs</th>
            </tr>
          </thead>
          <tbody>{rotation_runs_rows_html}</tbody>
        </table>
      </div>
    </div>
  </section>

  <section id="tab-predictions" class="tab-panel">
    <h2>Recent Predictions</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead><tr><th>ID</th><th>Timestamp</th><th>Symbol</th><th>Score</th><th>Entry Price</th><th>LLM Allowed</th><th>LLM Regime</th><th>LLM Risk</th><th>LLM Conf</th><th>LLM Reason</th></tr></thead>
          <tbody>{alert_rows}</tbody>
        </table>
      </div>
      <div class="pager">{alerts_pager}</div>
    </div>
  </section>

  <section id="tab-evals" class="tab-panel">
    <h2>Recent LLM Evaluations</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Timestamp</th><th>Symbol</th><th>Score</th><th>Spread %</th><th>Score OK</th><th>Cooldown OK</th><th>Alert Eligible</th>
              <th>LLM Verdict</th><th>LLM Regime</th><th>LLM Risk</th><th>LLM Conf</th><th>LLM Reason</th><th>Latency ms</th>
            </tr>
          </thead>
          <tbody>{llm_eval_rows}</tbody>
        </table>
      </div>
      <div class="pager">{evals_pager}</div>
    </div>
  </section>

  <section id="tab-diags" class="tab-panel">
    <h2>Freqtrade Entry Diagnostics</h2>
    <div class="section">
      <div class="muted">Source: {_esc(entry_diag_log_path)} | Rows loaded: {len(entry_diag_rows)}</div>
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th>Log TS</th><th>Pair</th><th>Candle</th><th>Status</th><th>Base OK</th><th>Final OK</th><th>Tag</th><th>Failed Checks</th>
              <th>RSI</th><th>ADX</th><th>ATR %</th><th>Spread %</th><th>Vol Z</th><th>Bench OK</th><th>TH RSI</th><th>TH ADX Min</th><th>TH ATR</th><th>TH Spread Min</th>
            </tr>
          </thead>
          <tbody>{entry_diag_rows_html}</tbody>
        </table>
      </div>
      <div class="pager">{diag_pager}</div>
    </div>
  </section>

  <section id="tab-outcomes" class="tab-panel">
    <h2>Recent Outcomes</h2>
    <div class="section">
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Alert ID</th><th>Symbol</th><th>Horizon (min)</th><th>Status</th>
              <th>Entry</th><th>Observed</th><th>Return</th><th>LLM Allowed</th><th>LLM Conf</th><th>Due</th><th>Resolved</th>
            </tr>
          </thead>
          <tbody>{outcome_rows}</tbody>
        </table>
      </div>
      <div class="pager">{outcomes_pager}</div>
    </div>
  </section>

  <section id="tab-debug" class="tab-panel">
    <h2>LLM Prompt / Response Debug</h2>
    <div class="section">
      <div class="muted">Source: bot-api `/debug/llm-calls`</div>
      {llm_debug_error_html}
      <div class="wrap">
        <table class="llm-debug-table">
          <thead>
            <tr>
              <th>Timestamp</th><th>Endpoint</th><th>Model</th><th>Parsed OK</th><th>Error</th><th>Prompt</th><th>Response</th>
            </tr>
          </thead>
          <tbody>{llm_debug_rows_html}</tbody>
        </table>
      </div>
      <div class="pager">{debug_pager}</div>
    </div>
  </section>

  <script>
    (function () {{
      const buttons = Array.from(document.querySelectorAll('.tab-btn'));
      const panels = Array.from(document.querySelectorAll('.tab-panel'));
      const validTabs = new Set(buttons.map((btn) => btn.dataset.tab).filter(Boolean));
      function normalizedTab(tabId) {{
        return validTabs.has(tabId) ? tabId : 'tab-overview';
      }}
      function tabUrl(tabId) {{
        const nextTab = normalizedTab(tabId);
        const url = new URL(window.location.href);
        url.searchParams.set('tab', nextTab);
        url.hash = `#${{nextTab}}`;
        return url;
      }}
      function syncTabTargets(tabId) {{
        const nextTab = normalizedTab(tabId);
        document.querySelectorAll('a[href]').forEach((node) => {{
          const href = node.getAttribute('href');
          if (!href) {{
            return;
          }}
          let url;
          try {{
            url = new URL(href, window.location.href);
          }} catch (_err) {{
            return;
          }}
          if (url.origin !== window.location.origin || url.pathname !== window.location.pathname) {{
            return;
          }}
          url.searchParams.set('tab', nextTab);
          node.setAttribute('href', `${{url.pathname}}${{url.search}}${{url.hash}}`);
        }});
        document.querySelectorAll('form[method=\"get\"]').forEach((form) => {{
          let input = form.querySelector('input[name=\"tab\"]');
          if (!input) {{
            input = document.createElement('input');
            input.type = 'hidden';
            input.name = 'tab';
            form.appendChild(input);
          }}
          input.value = nextTab;
        }});
      }}
      function activate(tabId, persist) {{
        const nextTab = normalizedTab(tabId);
        panels.forEach((panel) => panel.classList.toggle('active', panel.id === nextTab));
        buttons.forEach((btn) => btn.classList.toggle('active', btn.dataset.tab === nextTab));
        syncTabTargets(nextTab);
        try {{
          window.localStorage.setItem('spike-scanner.active-tab', nextTab);
        }} catch (_err) {{}}
        if (persist) {{
          const url = tabUrl(nextTab);
          history.replaceState(null, '', `${{url.pathname}}${{url.search}}${{url.hash}}`);
        }}
      }}
      buttons.forEach((btn) => {{
        btn.addEventListener('click', () => activate(btn.dataset.tab, true));
      }});
      let storedTab = '';
      try {{
        storedTab = window.localStorage.getItem('spike-scanner.active-tab') || '';
      }} catch (_err) {{}}
      const queryTab = new URLSearchParams(location.search).get('tab') || '';
      const hashTab = (location.hash || '').replace('#', '');
      activate(queryTab || hashTab || storedTab || 'tab-overview', Boolean(queryTab || hashTab || storedTab));
      window.addEventListener('popstate', () => {{
        const nextQueryTab = new URLSearchParams(location.search).get('tab') || '';
        const nextHashTab = (location.hash || '').replace('#', '');
        activate(nextQueryTab || nextHashTab || 'tab-overview', false);
      }});
      const timezoneEl = document.getElementById('client-timezone');
      const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'local';
      if (timezoneEl) {{
        timezoneEl.textContent = browserTimeZone;
      }}
      const formatter = new Intl.DateTimeFormat(undefined, {{
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      }});
      document.querySelectorAll('[data-utc-ts]').forEach((node) => {{
        const raw = node.getAttribute('data-utc-ts');
        if (!raw) {{
          return;
        }}
        const parsed = new Date(raw);
        if (Number.isNaN(parsed.getTime())) {{
          return;
        }}
        node.textContent = formatter.format(parsed);
        node.title = `UTC ${{raw}}`;
      }});
    }})();
  </script>
</body>
</html>
"""
    return web.Response(text=html, content_type="text/html")


async def api_alerts(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    limit = _parse_int(request.query.get("limit"), 200, 1, 2000)
    offset = _parse_int(request.query.get("offset"), 0, 0, 500000)
    symbol = _normalize_symbol(request.query.get("symbol"))
    llm_allowed = _parse_llm_verdict(request.query.get("llm_verdict", request.query.get("llm_allowed")))
    date_from = _parse_date(request.query.get("date_from"))
    date_to = _parse_date(request.query.get("date_to"))
    ts_from, ts_to = _date_bounds(date_from, date_to)
    total = store.count_alerts(symbol=symbol, llm_allowed=llm_allowed, ts_from=ts_from, ts_to=ts_to)
    items = store.fetch_recent_alerts(
        limit=limit,
        offset=offset,
        symbol=symbol,
        llm_allowed=llm_allowed,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    return web.json_response(
        {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_next": (offset + len(items)) < total,
        }
    )


async def api_outcomes(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    limit = _parse_int(request.query.get("limit"), 300, 1, 3000)
    offset = _parse_int(request.query.get("offset"), 0, 0, 500000)
    status = _parse_outcomes_status(request.query.get("status"))
    symbol = _normalize_symbol(request.query.get("symbol"))
    llm_allowed = _parse_llm_verdict(request.query.get("llm_verdict", request.query.get("llm_allowed")))
    date_from = _parse_date(request.query.get("date_from"))
    date_to = _parse_date(request.query.get("date_to"))
    ts_from, ts_to = _date_bounds(date_from, date_to)
    total = store.count_outcomes(
        status=status,
        symbol=symbol,
        llm_allowed=llm_allowed,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    items = store.fetch_recent_outcomes(
        limit=limit,
        status=status,
        offset=offset,
        symbol=symbol,
        llm_allowed=llm_allowed,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    return web.json_response(
        {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_next": (offset + len(items)) < total,
        }
    )


async def api_summary(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    return web.json_response(
        {
            "items": store.fetch_horizon_summary(),
            "llm_items": store.fetch_llm_outcome_summary(),
        }
    )


async def api_llm_evals(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    limit = _parse_int(request.query.get("limit"), 200, 1, 5000)
    offset = _parse_int(request.query.get("offset"), 0, 0, 500000)
    symbol = _normalize_symbol(request.query.get("symbol"))
    llm_allowed = _parse_llm_verdict(request.query.get("llm_verdict", request.query.get("llm_allowed")))
    date_from = _parse_date(request.query.get("date_from"))
    date_to = _parse_date(request.query.get("date_to"))
    ts_from, ts_to = _date_bounds(date_from, date_to)
    total = store.count_llm_shadow_evals(symbol=symbol, llm_allowed=llm_allowed, ts_from=ts_from, ts_to=ts_to)
    items = store.fetch_recent_llm_shadow_evals(
        limit=limit,
        offset=offset,
        symbol=symbol,
        llm_allowed=llm_allowed,
        ts_from=ts_from,
        ts_to=ts_to,
    )
    return web.json_response(
        {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_next": (offset + len(items)) < total,
        }
    )


async def api_entry_diags(request: web.Request) -> web.Response:
    limit = _parse_int(request.query.get("limit"), 200, 1, 5000)
    offset = _parse_int(request.query.get("offset"), 0, 0, 500000)
    symbol = _normalize_symbol(request.query.get("symbol"))
    date_from = _parse_date(request.query.get("date_from"))
    date_to = _parse_date(request.query.get("date_to"))
    ts_from, ts_to = _date_bounds(date_from, date_to)
    log_path = os.getenv("FREQTRADE_LOG_PATH", "/data/freqtrade.log")
    max_lines = _parse_int(os.getenv("FREQTRADE_DIAG_MAX_LINES"), 200000, 1000, 2000000)

    rows = _entry_diag_rows(log_path, max_lines)
    filtered = _filter_entry_diag_rows(rows, symbol=symbol, ts_from=ts_from, ts_to=ts_to)
    total = len(filtered)
    items = filtered[offset : offset + limit]

    return web.json_response(
        {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_next": (offset + len(items)) < total,
            "source": log_path,
        }
    )


async def api_llm_debug(request: web.Request) -> web.Response:
    limit = _parse_int(request.query.get("limit"), 200, 1, 500)
    offset = _parse_int(request.query.get("offset"), 0, 0, 500000)
    rows, error = await _fetch_llm_debug_rows(limit + offset)
    total = len(rows)
    items = rows[offset : offset + limit]
    return web.json_response(
        {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_next": (offset + len(items)) < total,
            "error": error,
        }
    )


async def api_daily_guard(_: web.Request) -> web.Response:
    return web.json_response(await _daily_guard_snapshot())


async def healthz(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app(store: PredictionStore) -> web.Application:
    app = web.Application()
    app["store"] = store
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", dashboard)
    app.router.add_get("/api/alerts", api_alerts)
    app.router.add_get("/api/outcomes", api_outcomes)
    app.router.add_get("/api/summary", api_summary)
    app.router.add_get("/api/llm-evals", api_llm_evals)
    app.router.add_get("/api/entry-diags", api_entry_diags)
    app.router.add_get("/api/llm-debug", api_llm_debug)
    app.router.add_get("/api/daily-guard", api_daily_guard)
    return app
