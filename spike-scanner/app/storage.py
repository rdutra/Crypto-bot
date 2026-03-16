import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


class PredictionStore:
    def __init__(self, jsonl_path: str, db_path: str, horizons_minutes: list[int]):
        self.jsonl_path = jsonl_path
        self.db_path = db_path
        self.horizons_minutes = horizons_minutes

        os.makedirs(os.path.dirname(self.jsonl_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                score REAL NOT NULL,
                entry_price REAL,
                meta_json TEXT NOT NULL,
                llm_allowed INTEGER,
                llm_regime TEXT,
                llm_risk_level TEXT,
                llm_confidence REAL,
                llm_note TEXT,
                llm_reason TEXT,
                llm_latency_ms INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                due_ts TEXT NOT NULL,
                resolved_ts TEXT,
                observed_price REAL,
                return_pct REAL,
                status TEXT NOT NULL DEFAULT 'pending',
                FOREIGN KEY(alert_id) REFERENCES alerts(id),
                UNIQUE(alert_id, horizon_minutes)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_shadow_evals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                score REAL NOT NULL,
                spread_pct REAL,
                threshold_ok INTEGER,
                cooldown_ok INTEGER,
                eligible_alert INTEGER,
                llm_allowed INTEGER,
                llm_regime TEXT,
                llm_risk_level TEXT,
                llm_confidence REAL,
                llm_reason TEXT,
                llm_latency_ms INTEGER
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_status_due ON outcomes(status, due_ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_shadow_evals_ts ON llm_shadow_evals(ts)")
        self._ensure_alert_columns()
        self.conn.commit()

    def _ensure_alert_columns(self) -> None:
        cur = self.conn.cursor()
        existing = {str(row["name"]) for row in cur.execute("PRAGMA table_info(alerts)").fetchall()}
        wanted: dict[str, str] = {
            "llm_allowed": "INTEGER",
            "llm_regime": "TEXT",
            "llm_risk_level": "TEXT",
            "llm_confidence": "REAL",
            "llm_note": "TEXT",
            "llm_reason": "TEXT",
            "llm_latency_ms": "INTEGER",
        }
        for column, col_type in wanted.items():
            if column in existing:
                continue
            cur.execute(f"ALTER TABLE alerts ADD COLUMN {column} {col_type}")

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _iso(dt: datetime) -> str:
        return dt.isoformat()

    @staticmethod
    def _from_iso(value: str) -> datetime:
        return datetime.fromisoformat(value)

    def write_prediction(self, payload: dict) -> int:
        payload = dict(payload)
        now = self._utc_now()
        ts = self._iso(now)
        payload["ts"] = ts

        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":")) + "\\n")

        symbol = str(payload.get("symbol", "")).upper()
        score = float(payload.get("score", 0.0))
        entry_price = payload.get("price")
        entry_price = float(entry_price) if entry_price is not None else None
        meta_json = json.dumps(payload.get("meta", {}), separators=(",", ":"))
        llm = payload.get("llm_shadow") if isinstance(payload.get("llm_shadow"), dict) else {}
        llm_allowed_raw = llm.get("allowed")
        if isinstance(llm_allowed_raw, bool):
            llm_allowed = 1 if llm_allowed_raw else 0
        else:
            llm_allowed = None
        llm_regime = str(llm.get("regime", ""))[:40] if llm else None
        llm_risk_level = str(llm.get("risk_level", ""))[:20] if llm else None
        try:
            llm_confidence = float(llm.get("confidence", 0.0)) if llm.get("confidence") is not None else None
        except (TypeError, ValueError):
            llm_confidence = None
        llm_note = str(llm.get("note", ""))[:220] if llm else None
        llm_reason = str(llm.get("reason", ""))[:80] if llm else None
        try:
            llm_latency_ms = int(llm.get("latency_ms")) if llm.get("latency_ms") is not None else None
        except (TypeError, ValueError):
            llm_latency_ms = None

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO alerts(
                ts, symbol, score, entry_price, meta_json,
                llm_allowed, llm_regime, llm_risk_level, llm_confidence, llm_note, llm_reason, llm_latency_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                symbol,
                score,
                entry_price,
                meta_json,
                llm_allowed,
                llm_regime,
                llm_risk_level,
                llm_confidence,
                llm_note,
                llm_reason,
                llm_latency_ms,
            ),
        )
        alert_id = int(cur.lastrowid)

        for horizon in self.horizons_minutes:
            due_ts = self._iso(now + timedelta(minutes=horizon))
            cur.execute(
                """
                INSERT OR IGNORE INTO outcomes(alert_id, symbol, horizon_minutes, due_ts, status)
                VALUES (?, ?, ?, ?, 'pending')
                """,
                (alert_id, symbol, int(horizon), due_ts),
            )

        self.conn.commit()
        return alert_id

    def resolve_due_outcomes(self, current_prices: dict[str, float], limit: int = 200) -> int:
        now = self._iso(self._utc_now())
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT o.id, o.symbol, o.horizon_minutes, a.entry_price
            FROM outcomes o
            JOIN alerts a ON a.id = o.alert_id
            WHERE o.status = 'pending' AND o.due_ts <= ?
            ORDER BY o.due_ts ASC
            LIMIT ?
            """,
            (now, int(limit)),
        ).fetchall()

        resolved = 0
        for row in rows:
            symbol = str(row["symbol"]).upper()
            entry_price = row["entry_price"]
            observed_price = current_prices.get(symbol)

            if entry_price is None or entry_price <= 0 or observed_price is None or observed_price <= 0:
                continue

            return_pct = ((observed_price / float(entry_price)) - 1.0) * 100.0
            cur.execute(
                """
                UPDATE outcomes
                SET resolved_ts = ?, observed_price = ?, return_pct = ?, status = 'resolved'
                WHERE id = ?
                """,
                (now, float(observed_price), float(return_pct), int(row["id"])),
            )
            resolved += 1

        if resolved:
            self.conn.commit()
        return resolved

    def write_llm_shadow_eval(self, payload: dict) -> int:
        payload = dict(payload)
        ts = payload.get("ts")
        if not ts:
            ts = self._iso(self._utc_now())

        symbol = str(payload.get("symbol", "")).upper()
        score = float(payload.get("score", 0.0))
        spread_pct = payload.get("spread_pct")
        spread_pct = float(spread_pct) if spread_pct is not None else None

        threshold_ok = 1 if bool(payload.get("threshold_ok", False)) else 0
        cooldown_ok = 1 if bool(payload.get("cooldown_ok", False)) else 0
        eligible_alert = 1 if bool(payload.get("eligible_alert", False)) else 0

        llm = payload.get("llm_shadow") if isinstance(payload.get("llm_shadow"), dict) else {}
        llm_allowed_raw = llm.get("allowed")
        if isinstance(llm_allowed_raw, bool):
            llm_allowed = 1 if llm_allowed_raw else 0
        else:
            llm_allowed = None
        llm_regime = str(llm.get("regime", ""))[:40] if llm else None
        llm_risk_level = str(llm.get("risk_level", ""))[:20] if llm else None
        try:
            llm_confidence = float(llm.get("confidence", 0.0)) if llm.get("confidence") is not None else None
        except (TypeError, ValueError):
            llm_confidence = None
        llm_reason = str(llm.get("reason", ""))[:80] if llm else None
        try:
            llm_latency_ms = int(llm.get("latency_ms")) if llm.get("latency_ms") is not None else None
        except (TypeError, ValueError):
            llm_latency_ms = None

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO llm_shadow_evals(
                ts, symbol, score, spread_pct, threshold_ok, cooldown_ok, eligible_alert,
                llm_allowed, llm_regime, llm_risk_level, llm_confidence, llm_reason, llm_latency_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                symbol,
                score,
                spread_pct,
                threshold_ok,
                cooldown_ok,
                eligible_alert,
                llm_allowed,
                llm_regime,
                llm_risk_level,
                llm_confidence,
                llm_reason,
                llm_latency_ms,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def fetch_recent_alerts(self, limit: int = 200) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT id, ts, symbol, score, entry_price, meta_json,
                   llm_allowed, llm_regime, llm_risk_level, llm_confidence, llm_note, llm_reason, llm_latency_ms
            FROM alerts
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        data = []
        for row in rows:
            item = dict(row)
            try:
                item["meta"] = json.loads(item.pop("meta_json", "{}"))
            except Exception:
                item["meta"] = {}
            data.append(item)
        return data

    def fetch_recent_outcomes(self, limit: int = 300, status: str | None = None) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        if status in {"pending", "resolved"}:
            rows = cur.execute(
                """
                SELECT o.id, o.alert_id, a.ts AS alert_ts, o.symbol, o.horizon_minutes,
                       a.entry_price, o.due_ts, o.resolved_ts, o.observed_price, o.return_pct, o.status,
                       a.llm_allowed, a.llm_regime, a.llm_risk_level, a.llm_confidence, a.llm_note, a.llm_reason
                FROM outcomes o
                JOIN alerts a ON a.id = o.alert_id
                WHERE o.status = ?
                ORDER BY o.id DESC
                LIMIT ?
                """,
                (status, int(limit)),
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT o.id, o.alert_id, a.ts AS alert_ts, o.symbol, o.horizon_minutes,
                       a.entry_price, o.due_ts, o.resolved_ts, o.observed_price, o.return_pct, o.status,
                       a.llm_allowed, a.llm_regime, a.llm_risk_level, a.llm_confidence, a.llm_note, a.llm_reason
                FROM outcomes o
                JOIN alerts a ON a.id = o.alert_id
                ORDER BY o.id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

        return [dict(row) for row in rows]

    def fetch_recent_llm_shadow_evals(self, limit: int = 200) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT id, ts, symbol, score, spread_pct, threshold_ok, cooldown_ok, eligible_alert,
                   llm_allowed, llm_regime, llm_risk_level, llm_confidence, llm_reason, llm_latency_ms
            FROM llm_shadow_evals
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [dict(row) for row in rows]

    def fetch_horizon_summary(self) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT
              horizon_minutes,
              COUNT(*) AS total,
              SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) AS resolved,
              AVG(CASE WHEN status='resolved' THEN return_pct END) AS avg_return_pct,
              AVG(CASE WHEN status='resolved' AND return_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM outcomes
            GROUP BY horizon_minutes
            ORDER BY horizon_minutes ASC
            """
        ).fetchall()

        summary = []
        for row in rows:
            item = dict(row)
            if item.get("avg_return_pct") is not None:
                item["avg_return_pct"] = float(item["avg_return_pct"])
            if item.get("win_rate") is not None:
                item["win_rate"] = float(item["win_rate"])
            summary.append(item)
        return summary

    def fetch_llm_outcome_summary(self) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT
              COALESCE(a.llm_allowed, -1) AS llm_allowed,
              o.horizon_minutes,
              COUNT(*) AS resolved,
              AVG(o.return_pct) AS avg_return_pct,
              AVG(CASE WHEN o.return_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM outcomes o
            JOIN alerts a ON a.id = o.alert_id
            WHERE o.status = 'resolved'
            GROUP BY COALESCE(a.llm_allowed, -1), o.horizon_minutes
            ORDER BY o.horizon_minutes ASC, llm_allowed DESC
            """
        ).fetchall()

        summary = []
        for row in rows:
            item = dict(row)
            if item.get("avg_return_pct") is not None:
                item["avg_return_pct"] = float(item["avg_return_pct"])
            if item.get("win_rate") is not None:
                item["win_rate"] = float(item["win_rate"])
            summary.append(item)
        return summary

    def close(self) -> None:
        self.conn.close()
