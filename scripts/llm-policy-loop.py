#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from spike_db import collect_spike_allowed_rate, normalize_postgres_target

LOGGER = logging.getLogger("llm-policy-loop")


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_postgres_db(target: str) -> bool:
    scheme = urlparse(target).scheme.lower()
    return scheme.startswith("postgres")


def _collect_trade_stats_sqlite(db_path: Path, lookback_hours: float) -> dict[str, Any]:
    stats = {
        "closed_trades": 0,
        "open_trades": 0,
        "wins": 0,
        "win_rate": 0.0,
        "avg_profit_pct": 0.0,
        "net_profit_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "market_note": "",
    }
    if not db_path.exists():
        stats["market_note"] = "trades_db_missing"
        return stats

    cutoff = (_utc_now() - timedelta(hours=lookback_hours)).isoformat()
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT
                COUNT(*) AS closed_trades,
                COALESCE(SUM(CASE WHEN close_profit > 0 THEN 1 ELSE 0 END), 0) AS wins,
                COALESCE(AVG(close_profit), 0.0) AS avg_profit_ratio,
                COALESCE(SUM(close_profit), 0.0) AS net_profit_ratio,
                COALESCE(MIN(close_profit), 0.0) AS min_profit_ratio
            FROM trades
            WHERE is_open = 0
              AND close_date IS NOT NULL
              AND julianday(close_date) >= julianday(?)
            """,
            (cutoff,),
        ).fetchone()
        open_row = cur.execute("SELECT COUNT(*) AS open_trades FROM trades WHERE is_open = 1").fetchone()
        conn.close()
    except Exception as exc:
        stats["market_note"] = f"trades_query_error:{str(exc)[:60]}"
        return stats

    closed = _safe_int(row["closed_trades"])
    wins = _safe_int(row["wins"])
    avg_profit_ratio = _safe_float(row["avg_profit_ratio"])
    net_profit_ratio = _safe_float(row["net_profit_ratio"])
    min_profit_ratio = _safe_float(row["min_profit_ratio"])
    open_trades = _safe_int(open_row["open_trades"])

    stats["closed_trades"] = closed
    stats["wins"] = wins
    stats["open_trades"] = open_trades
    stats["win_rate"] = (wins / closed) if closed > 0 else 0.0
    stats["avg_profit_pct"] = avg_profit_ratio * 100.0
    stats["net_profit_pct"] = net_profit_ratio * 100.0
    stats["max_drawdown_pct"] = min(0.0, min_profit_ratio * 100.0)
    if closed <= 0:
        stats["market_note"] = "no_recent_closed_trades"
    return stats


def _collect_trade_stats_postgres(db_url: str, lookback_hours: float) -> dict[str, Any]:
    stats = {
        "closed_trades": 0,
        "open_trades": 0,
        "wins": 0,
        "win_rate": 0.0,
        "avg_profit_pct": 0.0,
        "net_profit_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "market_note": "",
    }
    cutoff = _utc_now() - timedelta(hours=lookback_hours)
    try:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(normalize_postgres_target(db_url))
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                COUNT(*) AS closed_trades,
                COALESCE(SUM(CASE WHEN close_profit > 0 THEN 1 ELSE 0 END), 0) AS wins,
                COALESCE(AVG(close_profit), 0.0) AS avg_profit_ratio,
                COALESCE(SUM(close_profit), 0.0) AS net_profit_ratio,
                COALESCE(MIN(close_profit), 0.0) AS min_profit_ratio
            FROM trades
            WHERE is_open = false
              AND close_date IS NOT NULL
              AND close_date >= %s
            """,
            (cutoff,),
        )
        row = cur.fetchone() or {}
        cur.execute("SELECT COUNT(*) AS open_trades FROM trades WHERE is_open = true")
        open_row = cur.fetchone() or {}
        cur.close()
        conn.close()
    except Exception as exc:
        stats["market_note"] = f"trades_query_error:{str(exc)[:60]}"
        return stats

    closed = _safe_int(row.get("closed_trades"))
    wins = _safe_int(row.get("wins"))
    avg_profit_ratio = _safe_float(row.get("avg_profit_ratio"))
    net_profit_ratio = _safe_float(row.get("net_profit_ratio"))
    min_profit_ratio = _safe_float(row.get("min_profit_ratio"))
    open_trades = _safe_int(open_row.get("open_trades"))

    stats["closed_trades"] = closed
    stats["wins"] = wins
    stats["open_trades"] = open_trades
    stats["win_rate"] = (wins / closed) if closed > 0 else 0.0
    stats["avg_profit_pct"] = avg_profit_ratio * 100.0
    stats["net_profit_pct"] = net_profit_ratio * 100.0
    stats["max_drawdown_pct"] = min(0.0, min_profit_ratio * 100.0)
    if closed <= 0:
        stats["market_note"] = "no_recent_closed_trades"
    return stats


def _collect_trade_stats(db_target: str, lookback_hours: float) -> dict[str, Any]:
    if _is_postgres_db(db_target):
        return _collect_trade_stats_postgres(db_target, lookback_hours)
    return _collect_trade_stats_sqlite(Path(db_target), lookback_hours)


def _collect_rotation_metrics(log_path: Path, lookback_hours: float) -> Optional[dict[str, Any]]:
    if not log_path.exists():
        return None

    cutoff = _utc_now() - timedelta(hours=lookback_hours)
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return None

    for raw in reversed(lines[-200:]):
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        event = str(payload.get("event", "")).strip()
        if event not in {"rotation_decision", "rotation_no_candidates"}:
            continue

        ts_raw = str(payload.get("timestamp", "")).strip()
        if ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                if ts < cutoff:
                    continue
            except ValueError:
                pass

        decisions = payload.get("decisions", [])
        if not isinstance(decisions, list):
            decisions = []
        selected_pairs = {
            str(item).strip().upper()
            for item in payload.get("selected_pairs", [])
            if str(item).strip()
        }
        prefilter_rejected = payload.get("prefilter_rejected", [])
        if not isinstance(prefilter_rejected, list):
            prefilter_rejected = []
        candidate_count = payload.get("candidate_count")
        if candidate_count is None:
            candidate_count = len(decisions)
        candidate_count_int = max(0, _safe_int(candidate_count, len(decisions)))

        selected_decisions = []
        ranked_confidences: list[float] = []
        ranked_final_scores: list[float] = []
        for item in decisions:
            if not isinstance(item, dict):
                continue
            conf = _safe_float(item.get("confidence"), -1.0)
            final_score = _safe_float(item.get("final_score"), -1.0)
            if conf >= 0.0:
                ranked_confidences.append(conf)
            if final_score >= 0.0:
                ranked_final_scores.append(final_score)
            pair = str(item.get("pair", "")).strip().upper()
            if pair and pair in selected_pairs:
                selected_decisions.append(item)

        def _avg(values: list[float]) -> Optional[float]:
            if not values:
                return None
            return sum(values) / float(len(values))

        selected_confidences = [_safe_float(item.get("confidence"), -1.0) for item in selected_decisions]
        selected_confidences = [value for value in selected_confidences if value >= 0.0]
        selected_final_scores = [_safe_float(item.get("final_score"), -1.0) for item in selected_decisions]
        selected_final_scores = [value for value in selected_final_scores if value >= 0.0]

        selected_count = len(selected_pairs)
        selected_ratio = (selected_count / float(candidate_count_int)) if candidate_count_int > 0 else None
        return {
            "rotation_candidate_count": candidate_count_int,
            "rotation_selected_count": selected_count,
            "rotation_prefilter_rejected_count": len(prefilter_rejected),
            "rotation_selected_ratio": selected_ratio,
            "rotation_avg_selected_confidence": _avg(selected_confidences),
            "rotation_avg_selected_final_score": _avg(selected_final_scores),
            "rotation_avg_ranked_confidence": _avg(ranked_confidences),
            "rotation_avg_ranked_final_score": _avg(ranked_final_scores),
            "rotation_source": str(payload.get("source", "")).strip()[:40],
            "rotation_reason": str(payload.get("reason", "")).strip()[:120],
        }
    return None


def _local_policy_fallback(payload: dict[str, Any], reason: str) -> dict[str, Any]:
    closed = _safe_int(payload.get("closed_trades"), 0)
    win_rate = _safe_float(payload.get("win_rate"), 0.0)
    net_profit_pct = _safe_float(payload.get("net_profit_pct"), 0.0)
    max_drawdown_pct = _safe_float(payload.get("max_drawdown_pct"), 0.0)
    spike_allowed_rate = payload.get("spike_allowed_rate")
    spike_allowed = _safe_float(spike_allowed_rate, -1.0) if spike_allowed_rate is not None else None

    risk_flags = 0
    if closed >= 4 and win_rate < 0.45:
        risk_flags += 1
    if net_profit_pct <= -1.5:
        risk_flags += 1
    if max_drawdown_pct <= -2.0:
        risk_flags += 1
    if spike_allowed is not None and spike_allowed >= 0.0 and spike_allowed < 0.25:
        risk_flags += 1

    if risk_flags >= 2:
        return {
            "profile": "defensive",
            "confidence": 0.65,
            "note": "local_fallback:defensive",
            "aggr_entry_strictness": "strict",
            "risk_stake_multiplier": 0.35,
            "risk_max_open_trades": 1,
            "source": "fallback",
            "reason": reason,
        }
    if closed >= 6 and win_rate >= 0.58 and net_profit_pct >= 1.0:
        return {
            "profile": "offensive",
            "confidence": 0.62,
            "note": "local_fallback:offensive",
            "aggr_entry_strictness": "normal",
            "risk_stake_multiplier": 0.75,
            "risk_max_open_trades": 2,
            "source": "fallback",
            "reason": reason,
        }
    return {
        "profile": "normal",
        "confidence": 0.60,
        "note": "local_fallback:normal",
        "aggr_entry_strictness": "strict",
        "risk_stake_multiplier": 0.55,
        "risk_max_open_trades": 2,
        "source": "fallback",
        "reason": reason,
    }


def _normalize_policy(raw: dict[str, Any]) -> dict[str, Any]:
    profile = str(raw.get("profile", "normal")).strip().lower()
    if profile not in {"defensive", "normal", "offensive"}:
        profile = "normal"

    strictness = str(raw.get("aggr_entry_strictness", "strict")).strip().lower()
    if strictness not in {"strict", "normal"}:
        strictness = "strict" if profile != "offensive" else "normal"

    risk_stake = max(0.1, min(1.0, _safe_float(raw.get("risk_stake_multiplier"), 0.55)))
    risk_open = max(1, min(5, _safe_int(raw.get("risk_max_open_trades"), 2)))
    confidence = max(0.0, min(1.0, _safe_float(raw.get("confidence"), 0.0)))
    note = str(raw.get("note", "")).strip()[:220] or "runtime_policy"
    source = str(raw.get("source", "fallback")).strip().lower()
    if source not in {"llm", "fallback"}:
        source = "fallback"
    reason = str(raw.get("reason", "")).strip()[:120]

    return {
        "profile": profile,
        "confidence": confidence,
        "note": note,
        "aggr_entry_strictness": strictness,
        "risk_stake_multiplier": risk_stake,
        "risk_max_open_trades": risk_open,
        "source": source,
        "reason": reason,
    }


def _request_policy(bot_api_url: str, payload: dict[str, Any], timeout_seconds: float) -> Optional[Dict[str, Any]]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{bot_api_url.rstrip('/')}/policy",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            if response.status < 200 or response.status >= 300:
                return None
            raw = response.read().decode("utf-8")
            return json.loads(raw)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, Exception):
        return None


def _write_policy(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
    os.replace(tmp_path, path)


def _run_once(
    bot_api_url: str,
    trades_db_target: str,
    output_path: Path,
    lookback_hours: float,
    timeout_seconds: float,
    use_spike: bool,
    spike_db_target: str,
    rotation_log_path: Path,
) -> None:
    trade_stats = _collect_trade_stats(trades_db_target, lookback_hours)
    payload = {
        "lookback_hours": lookback_hours,
        "closed_trades": int(trade_stats["closed_trades"]),
        "win_rate": float(trade_stats["win_rate"]),
        "avg_profit_pct": float(trade_stats["avg_profit_pct"]),
        "net_profit_pct": float(trade_stats["net_profit_pct"]),
        "max_drawdown_pct": float(trade_stats["max_drawdown_pct"]),
        "open_trades": int(trade_stats["open_trades"]),
        "market_note": str(trade_stats.get("market_note", "")),
    }

    if use_spike:
        spike_allowed_rate = collect_spike_allowed_rate(spike_db_target, lookback_hours)
        if spike_allowed_rate is not None:
            payload["spike_allowed_rate"] = float(spike_allowed_rate)

    rotation_metrics = _collect_rotation_metrics(rotation_log_path, lookback_hours)
    if rotation_metrics:
        payload.update(rotation_metrics)

    policy = _request_policy(bot_api_url=bot_api_url, payload=payload, timeout_seconds=timeout_seconds)
    if not isinstance(policy, dict):
        policy = _local_policy_fallback(payload, reason="bot_api_unavailable")
    normalized = _normalize_policy(policy)
    normalized["generated_at"] = _utc_now().isoformat()
    normalized["metrics"] = payload
    _write_policy(output_path, normalized)

    LOGGER.info(
        "policy updated profile=%s strictness=%s stake=%.2f max_open=%s source=%s reason=%s closed=%s win_rate=%.2f net_pct=%.2f",
        normalized["profile"],
        normalized["aggr_entry_strictness"],
        float(normalized["risk_stake_multiplier"]),
        normalized["risk_max_open_trades"],
        normalized["source"],
        normalized.get("reason", ""),
        payload["closed_trades"],
        payload["win_rate"],
        payload["net_profit_pct"],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Periodic runtime risk-policy updater.")
    parser.add_argument("--once", action="store_true", help="Run a single policy update and exit.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    enabled = _env_bool("LLM_POLICY_LOOP_ENABLED", False)
    interval_minutes = max(1, _safe_int(os.getenv("LLM_POLICY_INTERVAL_MINUTES", "15"), 15))
    lookback_hours = max(1.0, _safe_float(os.getenv("LLM_POLICY_LOOKBACK_HOURS", "24"), 24.0))
    timeout_seconds = max(2.0, min(120.0, _safe_float(os.getenv("LLM_POLICY_HTTP_TIMEOUT_SECONDS", "45"), 45.0)))

    bot_api_url = os.getenv("LLM_BOT_API_URL", "http://bot-api:8000").strip() or "http://bot-api:8000"
    trades_db_target = (
        os.getenv("LLM_POLICY_TRADES_DB", "./freqtrade/user_data/tradesv3.sqlite").strip()
        or "./freqtrade/user_data/tradesv3.sqlite"
    )
    output_path = Path(
        os.getenv("LLM_POLICY_OUTPUT_PATH", "./freqtrade/user_data/logs/llm-runtime-policy.json").strip()
        or "./freqtrade/user_data/logs/llm-runtime-policy.json"
    )
    use_spike = _env_bool("LLM_POLICY_USE_SPIKE", True)
    spike_db_target = (
        os.getenv(
            "LLM_POLICY_SPIKE_DB_URL",
            os.getenv("LLM_POLICY_SPIKE_DB_PATH", "./freqtrade/user_data/logs/spike-scanner.sqlite"),
        ).strip()
        or "./freqtrade/user_data/logs/spike-scanner.sqlite"
    )
    rotation_log_path = Path(
        os.getenv("LLM_POLICY_ROTATION_LOG_PATH", "./freqtrade/user_data/logs/llm-pair-rotation.log").strip()
        or "./freqtrade/user_data/logs/llm-pair-rotation.log"
    )

    LOGGER.info(
        "starting llm-policy-loop enabled=%s interval_minutes=%s lookback_hours=%s output=%s",
        enabled,
        interval_minutes,
        lookback_hours,
        output_path,
    )

    if not enabled:
        LOGGER.info("LLM_POLICY_LOOP_ENABLED is false. Sleeping (set true to activate updates).")
        if args.once:
            return 0
        while True:
            time.sleep(300)

    while True:
        _run_once(
            bot_api_url=bot_api_url,
            trades_db_target=trades_db_target,
            output_path=output_path,
            lookback_hours=lookback_hours,
            timeout_seconds=timeout_seconds,
            use_spike=use_spike,
            spike_db_target=spike_db_target,
            rotation_log_path=rotation_log_path,
        )

        if args.once:
            return 0

        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    raise SystemExit(main())
