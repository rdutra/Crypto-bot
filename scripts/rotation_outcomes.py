#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

try:
    import ccxt
except Exception:  # pragma: no cover - optional at import time
    ccxt = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_load(raw: str) -> Any:
    return json.loads(raw) if raw else None


def _pair_to_symbol(pair: str) -> str:
    return pair.replace("/", "").upper()


def _as_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


@dataclass
class RotationOutcomeStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        self.conn.close()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rotation_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        self.conn.commit()

    def record(self, event: dict[str, Any], horizon_minutes: int) -> int:
        raw_run_ts = str(event.get("timestamp") or "").strip()
        run_ts = _parse_iso(raw_run_ts).isoformat() if raw_run_ts else ""
        if not run_ts:
            return 0
        due_ts = (_parse_iso(run_ts) + timedelta(minutes=max(1, horizon_minutes))).isoformat()
        cur = self.conn.cursor()
        inserted = 0
        for item in event.get("decisions", []):
            if not isinstance(item, dict):
                continue
            pair = str(item.get("pair", "")).strip().upper()
            if not pair:
                continue
            cur.execute(
                """
                INSERT OR IGNORE INTO rotation_outcomes(
                    run_ts, horizon_minutes, due_ts, pair, symbol,
                    selected, selection_status, selection_reason,
                    rotation_source, rotation_reason,
                    price, confidence, final_score, deterministic_score, atr_pct,
                    regime, risk_level, candidate_sources_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_ts,
                    int(horizon_minutes),
                    due_ts,
                    pair,
                    _pair_to_symbol(pair),
                    1 if bool(item.get("selected")) else 0,
                    str(item.get("selection_status", "") or ""),
                    str(item.get("selection_reason", "") or ""),
                    str(event.get("source", "") or ""),
                    str(event.get("reason", "") or ""),
                    _as_float(item.get("price")),
                    _as_float(item.get("confidence")),
                    _as_float(item.get("final_score")),
                    _as_float(item.get("deterministic_score")),
                    _as_float(item.get("atr_pct")),
                    str(item.get("regime", "") or ""),
                    str(item.get("risk_level", "") or ""),
                    json.dumps(item.get("candidate_sources", []) if isinstance(item.get("candidate_sources"), list) else []),
                ),
            )
            inserted += int(cur.rowcount or 0)
        self.conn.commit()
        return inserted

    def resolve(
        self,
        *,
        success_pct: float,
        exchange_id: str,
        rest_base_url: str,
        current_prices: dict[str, float],
        limit: int,
    ) -> dict[str, int]:
        now = _utc_now().isoformat()
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT id, pair, due_ts, price, selected
            FROM rotation_outcomes
            WHERE status = 'pending' AND due_ts <= ?
            ORDER BY due_ts ASC
            LIMIT ?
            """,
            (now, int(limit)),
        ).fetchall()
        if not rows:
            return {"resolved": 0, "missing_price": 0}

        prices = dict(current_prices)
        missing_pairs = [str(row["pair"]) for row in rows if str(row["pair"]) not in prices]
        if missing_pairs:
            prices.update(_fetch_prices(exchange_id=exchange_id, rest_base_url=rest_base_url, pairs=missing_pairs))

        resolved = 0
        missing_price = 0
        resolved_ts = _utc_now().isoformat()
        for row in rows:
            pair = str(row["pair"])
            entry_price = _as_float(row["price"])
            observed_price = _as_float(prices.get(pair))
            if entry_price is None or entry_price <= 0.0 or observed_price is None or observed_price <= 0.0:
                missing_price += 1
                continue
            return_pct = ((observed_price - entry_price) / entry_price) * 100.0
            success = 1 if return_pct >= success_pct else 0
            outcome_label = "success" if success else "miss"
            analysis_label = _analysis_label(selected=bool(row["selected"]), success=bool(success))
            cur.execute(
                """
                UPDATE rotation_outcomes
                SET status = 'resolved',
                    resolved_ts = ?,
                    observed_price = ?,
                    return_pct = ?,
                    success = ?,
                    outcome_label = ?,
                    analysis_label = ?
                WHERE id = ?
                """,
                (resolved_ts, observed_price, return_pct, success, outcome_label, analysis_label, int(row["id"])),
            )
            resolved += int(cur.rowcount or 0)
        self.conn.commit()
        return {"resolved": resolved, "missing_price": missing_price}


def _analysis_label(*, selected: bool, success: bool) -> str:
    if selected and success:
        return "true_positive"
    if selected and not success:
        return "false_positive"
    if not selected and success:
        return "false_negative"
    return "true_negative"


def _fetch_prices(*, exchange_id: str, rest_base_url: str, pairs: list[str]) -> dict[str, float]:
    fetched = _fetch_binance_prices(rest_base_url=rest_base_url, pairs=pairs)
    if fetched:
        return fetched
    return _fetch_ccxt_prices(exchange_id=exchange_id, pairs=pairs)


def _fetch_binance_prices(*, rest_base_url: str, pairs: list[str]) -> dict[str, float]:
    symbols = [_pair_to_symbol(pair) for pair in pairs if pair]
    if not symbols:
        return {}
    try:
        payload = json.dumps(sorted(set(symbols)), separators=(",", ":"))
        url = f"{rest_base_url.rstrip('/')}/api/v3/ticker/price?symbols={quote(payload)}"
        req = Request(url=url, method="GET", headers={"Accept": "application/json"})
        with urlopen(req, timeout=15.0) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return {}
    if not isinstance(body, list):
        return {}
    by_symbol = {}
    for item in body:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        price = _as_float(item.get("price"))
        if symbol and price is not None and price > 0.0:
            by_symbol[symbol] = price
    return {pair: by_symbol[_pair_to_symbol(pair)] for pair in pairs if _pair_to_symbol(pair) in by_symbol}


def _fetch_ccxt_prices(*, exchange_id: str, pairs: list[str]) -> dict[str, float]:
    if not pairs or ccxt is None:
        return {}
    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        return {}
    exchange = exchange_class({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    unique_pairs = []
    seen = set()
    for pair in pairs:
        norm = str(pair).strip().upper()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        unique_pairs.append(norm)
    fetched: dict[str, float] = {}
    try:
        tickers = exchange.fetch_tickers(unique_pairs)
        for pair in unique_pairs:
            ticker = tickers.get(pair) or {}
            price = _as_float(ticker.get("last"))
            if price is not None and price > 0.0:
                fetched[pair] = price
    except Exception:
        for pair in unique_pairs:
            try:
                ticker = exchange.fetch_ticker(pair)
            except Exception:
                continue
            price = _as_float(ticker.get("last"))
            if price is not None and price > 0.0:
                fetched[pair] = price
    finally:
        try:
            exchange.close()
        except Exception:
            pass
    return fetched


def _cmd_record(args: argparse.Namespace) -> int:
    event = _json_load(args.event_json)
    if not isinstance(event, dict):
        raise SystemExit("event-json must be a JSON object")
    store = RotationOutcomeStore(Path(args.db_path))
    try:
        inserted = store.record(event=event, horizon_minutes=int(args.horizon_minutes))
    finally:
        store.close()
    print(json.dumps({"inserted": inserted}, separators=(",", ":")))
    return 0


def _cmd_resolve(args: argparse.Namespace) -> int:
    current_prices_raw = _json_load(args.current_prices_json) or {}
    current_prices = {}
    if isinstance(current_prices_raw, dict):
        for pair, value in current_prices_raw.items():
            price = _as_float(value)
            if price is not None and price > 0.0:
                current_prices[str(pair).strip().upper()] = price
    store = RotationOutcomeStore(Path(args.db_path))
    try:
        result = store.resolve(
            success_pct=float(args.success_pct),
            exchange_id=str(args.exchange).strip().lower(),
            rest_base_url=str(args.rest_base_url).strip(),
            current_prices=current_prices,
            limit=int(args.limit),
        )
    finally:
        store.close()
    print(json.dumps(result, separators=(",", ":")))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track rotation candidate outcomes independently from executed trades.")
    sub = parser.add_subparsers(dest="command", required=True)

    record = sub.add_parser("record", help="Record ranked rotation decisions for later resolution.")
    record.add_argument("--db-path", required=True)
    record.add_argument("--event-json", required=True)
    record.add_argument("--horizon-minutes", type=int, default=60)
    record.set_defaults(func=_cmd_record)

    resolve = sub.add_parser("resolve", help="Resolve due rotation outcomes using current prices.")
    resolve.add_argument("--db-path", required=True)
    resolve.add_argument("--exchange", default="binance")
    resolve.add_argument("--rest-base-url", default="https://api.binance.com")
    resolve.add_argument("--success-pct", type=float, default=1.0)
    resolve.add_argument("--limit", type=int, default=200)
    resolve.add_argument("--current-prices-json", default="{}")
    resolve.set_defaults(func=_cmd_resolve)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
