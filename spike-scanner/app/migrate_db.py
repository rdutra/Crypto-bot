from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from app.storage import PredictionStore


def _normalize_postgres_target(target: str) -> str:
    normalized = str(target).strip()
    if normalized.startswith("postgresql+psycopg2://"):
        return "postgresql://" + normalized[len("postgresql+psycopg2://") :]
    if normalized.startswith("postgresql+psycopg://"):
        return "postgresql://" + normalized[len("postgresql+psycopg://") :]
    return normalized


def _database_name(target: str) -> str:
    parsed = urlparse(_normalize_postgres_target(target))
    return parsed.path.lstrip("/") or "postgres"


def _ensure_database(admin_url: str, target_url: str) -> None:
    db_name = _database_name(target_url)
    conn = psycopg2.connect(_normalize_postgres_target(admin_url))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}") .format(sql.Identifier(db_name)))
    cur.close()
    conn.close()


def _set_sequence(cur, table: str) -> None:
    cur.execute(
        "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM {}), 1), true)".format(table),
        (table,),
    )


def _load_rows(conn: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    cur = conn.cursor()
    rows = cur.execute(f"SELECT * FROM {table} ORDER BY id ASC").fetchall()
    cur.close()
    return rows


def migrate(source_path: Path, target_url: str, admin_url: str) -> None:
    if not source_path.exists():
        raise FileNotFoundError(f"missing spike sqlite db: {source_path}")

    _ensure_database(admin_url=admin_url, target_url=target_url)

    bootstrap_store = PredictionStore(jsonl_path="/tmp/spike-migrate-bootstrap.jsonl", db_path=target_url, horizons_minutes=[60])
    bootstrap_store.close()

    sqlite_conn = sqlite3.connect(str(source_path))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(_normalize_postgres_target(target_url))
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor()

    alert_rows = _load_rows(sqlite_conn, "alerts")
    for row in alert_rows:
        pg_cur.execute(
            """
            INSERT INTO alerts(
                id, ts, symbol, score, entry_price, meta_json,
                llm_allowed, llm_regime, llm_risk_level, llm_confidence, llm_note, llm_reason, llm_latency_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                ts = EXCLUDED.ts,
                symbol = EXCLUDED.symbol,
                score = EXCLUDED.score,
                entry_price = EXCLUDED.entry_price,
                meta_json = EXCLUDED.meta_json,
                llm_allowed = EXCLUDED.llm_allowed,
                llm_regime = EXCLUDED.llm_regime,
                llm_risk_level = EXCLUDED.llm_risk_level,
                llm_confidence = EXCLUDED.llm_confidence,
                llm_note = EXCLUDED.llm_note,
                llm_reason = EXCLUDED.llm_reason,
                llm_latency_ms = EXCLUDED.llm_latency_ms
            """,
            tuple(row),
        )

    outcome_rows = _load_rows(sqlite_conn, "outcomes")
    for row in outcome_rows:
        pg_cur.execute(
            """
            INSERT INTO outcomes(
                id, alert_id, symbol, horizon_minutes, due_ts, resolved_ts,
                observed_price, return_pct, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                alert_id = EXCLUDED.alert_id,
                symbol = EXCLUDED.symbol,
                horizon_minutes = EXCLUDED.horizon_minutes,
                due_ts = EXCLUDED.due_ts,
                resolved_ts = EXCLUDED.resolved_ts,
                observed_price = EXCLUDED.observed_price,
                return_pct = EXCLUDED.return_pct,
                status = EXCLUDED.status
            """,
            tuple(row),
        )

    eval_rows = _load_rows(sqlite_conn, "llm_shadow_evals")
    for row in eval_rows:
        pg_cur.execute(
            """
            INSERT INTO llm_shadow_evals(
                id, ts, symbol, score, spread_pct, threshold_ok, cooldown_ok, eligible_alert,
                llm_allowed, llm_regime, llm_risk_level, llm_confidence, llm_reason, llm_latency_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                ts = EXCLUDED.ts,
                symbol = EXCLUDED.symbol,
                score = EXCLUDED.score,
                spread_pct = EXCLUDED.spread_pct,
                threshold_ok = EXCLUDED.threshold_ok,
                cooldown_ok = EXCLUDED.cooldown_ok,
                eligible_alert = EXCLUDED.eligible_alert,
                llm_allowed = EXCLUDED.llm_allowed,
                llm_regime = EXCLUDED.llm_regime,
                llm_risk_level = EXCLUDED.llm_risk_level,
                llm_confidence = EXCLUDED.llm_confidence,
                llm_reason = EXCLUDED.llm_reason,
                llm_latency_ms = EXCLUDED.llm_latency_ms
            """,
            tuple(row),
        )

    _set_sequence(pg_cur, "alerts")
    _set_sequence(pg_cur, "outcomes")
    _set_sequence(pg_cur, "llm_shadow_evals")
    pg_conn.commit()

    sqlite_conn.close()
    pg_cur.close()
    pg_conn.close()

    print(
        f"Migrated spike data: alerts={len(alert_rows)} outcomes={len(outcome_rows)} llm_shadow_evals={len(eval_rows)}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate spike-scanner SQLite DB to Postgres.")
    parser.add_argument("--from", dest="source_path", required=True)
    parser.add_argument("--to", dest="target_url", required=True)
    parser.add_argument("--admin-url", dest="admin_url", required=True)
    args = parser.parse_args()
    migrate(source_path=Path(args.source_path), target_url=args.target_url, admin_url=args.admin_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
