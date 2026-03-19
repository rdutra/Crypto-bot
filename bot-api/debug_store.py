import logging
import os
import sqlite3
from threading import Lock
from typing import Dict, List, Tuple


LOGGER = logging.getLogger("bot-api.debug-store")


class LlmDebugStore:
    def __init__(self, *, enabled: bool, db_path: str, max_rows: int) -> None:
        self.enabled = bool(enabled)
        self.db_path = str(db_path).strip()
        self.max_rows = max(1000, int(max_rows))
        self._lock = Lock()
        self._conn: sqlite3.Connection | None = None
        self._write_count = 0
        if self.enabled:
            self._init()

    @property
    def available(self) -> bool:
        return self.enabled and self._conn is not None

    def _init(self) -> None:
        if not self.db_path:
            self.enabled = False
            return

        try:
            parent = os.path.dirname(self.db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS llm_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    model TEXT NOT NULL,
                    parsed_ok INTEGER NOT NULL,
                    error TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    response TEXT NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_ts ON llm_calls (ts DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_endpoint ON llm_calls (endpoint);")
            conn.commit()
            self._conn = conn
            LOGGER.info("LLM debug persistence enabled: path=%s max_rows=%s", self.db_path, self.max_rows)
        except Exception as exc:
            self.enabled = False
            self._conn = None
            LOGGER.warning("LLM debug persistence disabled: failed to init sqlite (%s)", exc)

    def insert(self, entry: Dict[str, object]) -> None:
        if not self.available:
            return

        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.execute(
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
                LOGGER.warning("LLM debug sqlite insert failed: %s", exc)

    def _prune_locked(self) -> None:
        if self._conn is None:
            return
        try:
            cursor = self._conn.execute(
                "SELECT id FROM llm_calls ORDER BY id DESC LIMIT 1 OFFSET ?",
                (self.max_rows - 1,),
            )
            row = cursor.fetchone()
            if row is None:
                return
            cutoff_id = int(row[0])
            self._conn.execute("DELETE FROM llm_calls WHERE id < ?", (cutoff_id,))
        except Exception as exc:
            LOGGER.warning("LLM debug sqlite prune failed: %s", exc)

    def fetch(self, *, limit: int, endpoint: str | None = None) -> Tuple[List[Dict[str, object]], int]:
        if not self.available:
            return [], 0

        endpoint_norm = str(endpoint or "").strip().lower()
        with self._lock:
            if self._conn is None:
                return [], 0
            try:
                if endpoint_norm:
                    total = int(
                        self._conn.execute(
                            "SELECT COUNT(*) FROM llm_calls WHERE lower(endpoint) = ?",
                            (endpoint_norm,),
                        ).fetchone()[0]
                    )
                    rows = self._conn.execute(
                        """
                        SELECT ts, endpoint, model, parsed_ok, error, prompt, response
                        FROM llm_calls
                        WHERE lower(endpoint) = ?
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (endpoint_norm, int(limit)),
                    ).fetchall()
                else:
                    total = int(self._conn.execute("SELECT COUNT(*) FROM llm_calls").fetchone()[0])
                    rows = self._conn.execute(
                        """
                        SELECT ts, endpoint, model, parsed_ok, error, prompt, response
                        FROM llm_calls
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (int(limit),),
                    ).fetchall()
            except Exception as exc:
                LOGGER.warning("LLM debug sqlite fetch failed: %s", exc)
                return [], 0

        items: List[Dict[str, object]] = []
        for row in rows:
            items.append(
                {
                    "ts": str(row["ts"]),
                    "endpoint": str(row["endpoint"]),
                    "model": str(row["model"]),
                    "parsed_ok": bool(row["parsed_ok"]),
                    "error": str(row["error"]),
                    "prompt": str(row["prompt"]),
                    "response": str(row["response"]),
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
