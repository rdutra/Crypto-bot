from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def is_postgres_target(target: str) -> bool:
    scheme = urlparse(str(target).strip()).scheme.lower()
    return scheme.startswith("postgres")


def normalize_postgres_target(target: str) -> str:
    normalized = str(target).strip()
    if normalized.startswith("postgresql+psycopg2://"):
        return "postgresql://" + normalized[len("postgresql+psycopg2://") :]
    if normalized.startswith("postgresql+psycopg://"):
        return "postgresql://" + normalized[len("postgresql+psycopg://") :]
    return normalized


def _sqlite_exists(target: str) -> bool:
    return Path(target).exists()


def _fetch_spike_bias_rows_sqlite(target: str) -> list[dict[str, Any]]:
    if not _sqlite_exists(target):
        return []
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT ts, symbol, score, llm_allowed, 1 AS source_rank, 1 AS eligible_rank FROM alerts ORDER BY id DESC LIMIT 2000"
    ).fetchall()
    if not rows:
        rows = conn.execute(
            """
            SELECT ts, symbol, score, llm_allowed,
                   0 AS source_rank,
                   CASE WHEN eligible_alert = 1 THEN 1 ELSE 0 END AS eligible_rank
            FROM llm_shadow_evals
            ORDER BY id DESC
            LIMIT 4000
            """
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def _fetch_spike_bias_rows_postgres(target: str) -> list[dict[str, Any]]:
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(normalize_postgres_target(target))
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT ts, symbol, score, llm_allowed, 1 AS source_rank, 1 AS eligible_rank FROM alerts ORDER BY id DESC LIMIT 2000"
    )
    rows = cur.fetchall() or []
    if not rows:
        cur.execute(
            """
            SELECT ts, symbol, score, llm_allowed,
                   0 AS source_rank,
                   CASE WHEN eligible_alert = 1 THEN 1 ELSE 0 END AS eligible_rank
            FROM llm_shadow_evals
            ORDER BY id DESC
            LIMIT 4000
            """
        )
        rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return [dict(row) for row in rows]


def fetch_spike_bias_rows(target: str) -> list[dict[str, Any]]:
    target = str(target).strip()
    if not target:
        return []
    if is_postgres_target(target):
        return _fetch_spike_bias_rows_postgres(target)
    return _fetch_spike_bias_rows_sqlite(target)


def _collect_spike_allowed_rate_sqlite(target: str, lookback_hours: float) -> Optional[float]:
    if not _sqlite_exists(target):
        return None
    cutoff = (_utc_now() - timedelta(hours=lookback_hours)).isoformat()
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN llm_allowed = 1 THEN 1 ELSE 0 END), 0) AS allowed
        FROM llm_shadow_evals
        WHERE julianday(ts) >= julianday(?)
        """,
        (cutoff,),
    ).fetchone()
    conn.close()
    total = _safe_int(row["total"] if row else 0)
    allowed = _safe_int(row["allowed"] if row else 0)
    if total <= 0:
        return None
    return max(0.0, min(1.0, allowed / total))


def _collect_spike_allowed_rate_postgres(target: str, lookback_hours: float) -> Optional[float]:
    import psycopg2
    import psycopg2.extras

    cutoff = (_utc_now() - timedelta(hours=lookback_hours)).isoformat()
    conn = psycopg2.connect(normalize_postgres_target(target))
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN llm_allowed = 1 THEN 1 ELSE 0 END), 0) AS allowed
        FROM llm_shadow_evals
        WHERE ts >= %s
        """,
        (cutoff,),
    )
    row = cur.fetchone() or {}
    cur.close()
    conn.close()
    total = _safe_int(row.get("total"))
    allowed = _safe_int(row.get("allowed"))
    if total <= 0:
        return None
    return max(0.0, min(1.0, allowed / total))


def collect_spike_allowed_rate(target: str, lookback_hours: float) -> Optional[float]:
    target = str(target).strip()
    if not target:
        return None
    if is_postgres_target(target):
        return _collect_spike_allowed_rate_postgres(target, lookback_hours)
    return _collect_spike_allowed_rate_sqlite(target, lookback_hours)
