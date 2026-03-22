import logging
import os
import sqlite3
from threading import Lock
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

LOGGER = logging.getLogger("bot-api.debug-store")


class LlmDebugStore:
    def __init__(self, *, enabled: bool, db_path: str, max_rows: int) -> None:
        self.enabled = bool(enabled)
        self.db_path = str(db_path).strip()
        self.max_rows = max(1000, int(max_rows))
        self.backend = self._detect_backend(self.db_path)
        self._lock = Lock()
        self._conn: Any = None
        self._write_count = 0
        if self.enabled:
            self._init()

    @property
    def available(self) -> bool:
        return self.enabled and self._conn is not None

    @property
    def storage_kind(self) -> str:
        if not self.enabled:
            return "disabled"
        if self.available:
            return self.backend
        return "memory"

    @staticmethod
    def _detect_backend(target: str) -> str:
        scheme = urlparse(str(target).strip()).scheme.lower()
        return "postgres" if scheme.startswith("postgres") else "sqlite"

    @staticmethod
    def _normalize_postgres_target(target: str) -> str:
        normalized = str(target).strip()
        if normalized.startswith("postgresql+psycopg2://"):
            return "postgresql://" + normalized[len("postgresql+psycopg2://") :]
        if normalized.startswith("postgresql+psycopg://"):
            return "postgresql://" + normalized[len("postgresql+psycopg://") :]
        return normalized

    def _sql(self, query: str) -> str:
        if self.backend == "postgres":
            return query.replace("?", "%s")
        return query

    def _execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        if self._conn is None:
            return
        if self.backend == "sqlite":
            self._conn.execute(query, params)
            return
        cur = self._conn.cursor()
        cur.execute(self._sql(query), params)
        cur.close()

    def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        if self._conn is None:
            return None
        if self.backend == "sqlite":
            return self._conn.execute(query, params).fetchone()
        cur = self._conn.cursor()
        cur.execute(self._sql(query), params)
        row = cur.fetchone()
        cur.close()
        return row

    def _fetchall(self, query: str, params: tuple[Any, ...] = ()) -> list[Any]:
        if self._conn is None:
            return []
        if self.backend == "sqlite":
            return self._conn.execute(query, params).fetchall()
        cur = self._conn.cursor()
        cur.execute(self._sql(query), params)
        rows = cur.fetchall()
        cur.close()
        return rows

    def _row_value(self, row: Any, key: str, index: int) -> Any:
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(key)
        return row[index]

    def _init(self) -> None:
        if not self.db_path:
            self.enabled = False
            return

        try:
            if self.backend == "sqlite":
                parent = os.path.dirname(self.db_path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
            else:
                import psycopg2
                import psycopg2.extras

                conn = psycopg2.connect(self._normalize_postgres_target(self.db_path))
            self._conn = conn
            self._execute(
                """
                CREATE TABLE IF NOT EXISTS llm_calls (
                    id {id_type},
                    ts TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    model TEXT NOT NULL,
                    parsed_ok INTEGER NOT NULL,
                    error TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    response TEXT NOT NULL
                )
                """.replace("{id_type}", "INTEGER PRIMARY KEY AUTOINCREMENT" if self.backend == "sqlite" else "BIGSERIAL PRIMARY KEY")
            )
            self._execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_ts ON llm_calls (ts DESC)")
            self._execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_endpoint ON llm_calls (endpoint)")
            self._conn.commit()
            LOGGER.info(
                "LLM debug persistence enabled: backend=%s target=%s max_rows=%s",
                self.backend,
                self.db_path,
                self.max_rows,
            )
        except Exception as exc:
            self.enabled = False
            self._conn = None
            LOGGER.warning("LLM debug persistence disabled: failed to init %s (%s)", self.backend, exc)

    def insert(self, entry: Dict[str, object]) -> None:
        if not self.available:
            return

        with self._lock:
            if self._conn is None:
                return
            try:
                self._execute(
                    """
                    INSERT INTO llm_calls (ts, endpoint, model, parsed_ok, error, prompt, response)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(entry.get("ts", "")),
                        str(entry.get("endpoint", "")),
                        str(entry.get("model", "")),
                        1 if bool(entry.get("parsed_ok", False)) else 0,
                        str(entry.get("error", "")),
                        str(entry.get("prompt", "")),
                        str(entry.get("response", "")),
                    ),
                )
                self._write_count += 1
                if self._write_count % 100 == 0:
                    self._prune_locked()
                self._conn.commit()
            except Exception as exc:
                LOGGER.warning("LLM debug %s insert failed: %s", self.backend, exc)

    def _prune_locked(self) -> None:
        if self._conn is None:
            return
        try:
            row = self._fetchone(
                "SELECT id FROM llm_calls ORDER BY id DESC LIMIT 1 OFFSET ?",
                (self.max_rows - 1,),
            )
            if row is None:
                return
            cutoff_id = int(self._row_value(row, "id", 0))
            self._execute("DELETE FROM llm_calls WHERE id < ?", (cutoff_id,))
        except Exception as exc:
            LOGGER.warning("LLM debug %s prune failed: %s", self.backend, exc)

    def fetch(self, *, limit: int, endpoint: str | None = None) -> Tuple[List[Dict[str, object]], int]:
        if not self.available:
            return [], 0

        endpoint_norm = str(endpoint or "").strip().lower()
        with self._lock:
            if self._conn is None:
                return [], 0
            try:
                if endpoint_norm:
                    total_row = self._fetchone(
                        "SELECT COUNT(*) FROM llm_calls WHERE lower(endpoint) = ?",
                        (endpoint_norm,),
                    )
                    total = int(self._row_value(total_row, "count", 0) or 0)
                    rows = self._fetchall(
                        """
                        SELECT ts, endpoint, model, parsed_ok, error, prompt, response
                        FROM llm_calls
                        WHERE lower(endpoint) = ?
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (endpoint_norm, int(limit)),
                    )
                else:
                    total_row = self._fetchone("SELECT COUNT(*) FROM llm_calls")
                    total = int(self._row_value(total_row, "count", 0) or 0)
                    rows = self._fetchall(
                        """
                        SELECT ts, endpoint, model, parsed_ok, error, prompt, response
                        FROM llm_calls
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (int(limit),),
                    )
            except Exception as exc:
                LOGGER.warning("LLM debug %s fetch failed: %s", self.backend, exc)
                return [], 0

        items: List[Dict[str, object]] = []
        for row in rows:
            items.append(
                {
                    "ts": str(self._row_value(row, "ts", 0)),
                    "endpoint": str(self._row_value(row, "endpoint", 1)),
                    "model": str(self._row_value(row, "model", 2)),
                    "parsed_ok": bool(self._row_value(row, "parsed_ok", 3)),
                    "error": str(self._row_value(row, "error", 4)),
                    "prompt": str(self._row_value(row, "prompt", 5)),
                    "response": str(self._row_value(row, "response", 6)),
                }
            )
        return items, total

    def close(self) -> None:
        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
