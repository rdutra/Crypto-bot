#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql


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
        sql.SQL(
            "SELECT setval(pg_get_serial_sequence({}, 'id'), COALESCE((SELECT MAX(id) FROM {}), 1), true)"
        ).format(sql.Literal(table), sql.Identifier(table))
    )


def _migrate_llm_debug(cur, sqlite_path: Path) -> int:
    if not sqlite_path.exists():
        return 0
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, ts, endpoint, model, parsed_ok, error, prompt, response FROM llm_calls ORDER BY id ASC").fetchall()
    for row in rows:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_calls (
                id BIGSERIAL PRIMARY KEY,
                ts TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                model TEXT NOT NULL,
                parsed_ok INTEGER NOT NULL,
                error TEXT NOT NULL,
                prompt TEXT NOT NULL,
                response TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_ts ON llm_calls (ts DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_endpoint ON llm_calls (endpoint)")
        cur.execute(
            """
            INSERT INTO llm_calls (id, ts, endpoint, model, parsed_ok, error, prompt, response)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                ts = EXCLUDED.ts,
                endpoint = EXCLUDED.endpoint,
                model = EXCLUDED.model,
                parsed_ok = EXCLUDED.parsed_ok,
                error = EXCLUDED.error,
                prompt = EXCLUDED.prompt,
                response = EXCLUDED.response
            """,
            tuple(row),
        )
    conn.close()
    _set_sequence(cur, "llm_calls")
    return len(rows)


def _migrate_rotation_outcomes(cur, sqlite_path: Path) -> int:
    if not sqlite_path.exists():
        return 0
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM rotation_outcomes ORDER BY id ASC").fetchall()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rotation_outcomes (
            id BIGSERIAL PRIMARY KEY,
            run_ts TEXT NOT NULL,
            horizon_minutes INTEGER NOT NULL,
            due_ts TEXT NOT NULL,
            pair TEXT NOT NULL,
            symbol TEXT NOT NULL,
            selected INTEGER NOT NULL,
            selection_status TEXT NOT NULL,
            selection_reason TEXT NOT NULL,
            rotation_source TEXT NOT NULL,
            rotation_reason TEXT NOT NULL,
            price REAL,
            confidence REAL,
            final_score REAL,
            deterministic_score REAL,
            atr_pct REAL,
            regime TEXT,
            risk_level TEXT,
            candidate_sources_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            resolved_ts TEXT,
            observed_price REAL,
            return_pct REAL,
            success INTEGER,
            outcome_label TEXT,
            analysis_label TEXT,
            UNIQUE(run_ts, pair, horizon_minutes)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rotation_outcomes_status_due ON rotation_outcomes(status, due_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rotation_outcomes_pair_run ON rotation_outcomes(pair, run_ts)")
    for row in rows:
        cur.execute(
            """
            INSERT INTO rotation_outcomes (
                id, run_ts, horizon_minutes, due_ts, pair, symbol, selected,
                selection_status, selection_reason, rotation_source, rotation_reason,
                price, confidence, final_score, deterministic_score, atr_pct,
                regime, risk_level, candidate_sources_json, status, resolved_ts,
                observed_price, return_pct, success, outcome_label, analysis_label
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                run_ts = EXCLUDED.run_ts,
                horizon_minutes = EXCLUDED.horizon_minutes,
                due_ts = EXCLUDED.due_ts,
                pair = EXCLUDED.pair,
                symbol = EXCLUDED.symbol,
                selected = EXCLUDED.selected,
                selection_status = EXCLUDED.selection_status,
                selection_reason = EXCLUDED.selection_reason,
                rotation_source = EXCLUDED.rotation_source,
                rotation_reason = EXCLUDED.rotation_reason,
                price = EXCLUDED.price,
                confidence = EXCLUDED.confidence,
                final_score = EXCLUDED.final_score,
                deterministic_score = EXCLUDED.deterministic_score,
                atr_pct = EXCLUDED.atr_pct,
                regime = EXCLUDED.regime,
                risk_level = EXCLUDED.risk_level,
                candidate_sources_json = EXCLUDED.candidate_sources_json,
                status = EXCLUDED.status,
                resolved_ts = EXCLUDED.resolved_ts,
                observed_price = EXCLUDED.observed_price,
                return_pct = EXCLUDED.return_pct,
                success = EXCLUDED.success,
                outcome_label = EXCLUDED.outcome_label,
                analysis_label = EXCLUDED.analysis_label
            """,
            tuple(row),
        )
    conn.close()
    _set_sequence(cur, "rotation_outcomes")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate analytics SQLite stores to Postgres.")
    parser.add_argument("--target-url", required=True)
    parser.add_argument("--admin-url", required=True)
    parser.add_argument("--llm-debug-sqlite", required=True)
    parser.add_argument("--rotation-outcomes-sqlite", required=True)
    args = parser.parse_args()

    _ensure_database(admin_url=args.admin_url, target_url=args.target_url)
    conn = psycopg2.connect(_normalize_postgres_target(args.target_url))
    cur = conn.cursor()
    llm_rows = _migrate_llm_debug(cur, Path(args.llm_debug_sqlite))
    rotation_rows = _migrate_rotation_outcomes(cur, Path(args.rotation_outcomes_sqlite))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Migrated analytics data: llm_calls={llm_rows} rotation_outcomes={rotation_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
