from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .common import parse_pairs


def risk_changed(args: argparse.Namespace) -> int:
    old = parse_pairs(args.current_risk_pairs)
    new = parse_pairs(args.selected_pairs)
    print("true" if old != new else "false")
    return 0



def set_env_value(args: argparse.Namespace) -> int:
    lines = args.env_path.read_text().splitlines() if args.env_path.exists() else []
    found = False
    for index, raw in enumerate(lines):
        stripped = raw.strip()
        if stripped.startswith("#") or "=" not in raw:
            continue
        current_key, _ = raw.split("=", 1)
        if current_key.strip() != args.key:
            continue
        lines[index] = f"{args.key}={args.value}"
        found = True
        break
    if not found:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(f"{args.key}={args.value}")
    args.env_path.write_text("\n".join(lines) + "\n")
    return 0



def sync_whitelist(args: argparse.Namespace) -> int:
    if not args.config_path.exists():
        print(f"Config not found: {args.config_path}", file=sys.stderr)
        return 1
    cfg = json.loads(args.config_path.read_text())
    exchange = cfg.setdefault("exchange", {})
    ordered = []
    seen = set()
    for pair in parse_pairs(args.core_pairs) + parse_pairs(args.selected_pairs):
        if pair in seen:
            continue
        seen.add(pair)
        ordered.append(pair)
    exchange["pair_whitelist"] = ordered
    args.config_path.write_text(json.dumps(cfg, indent=2) + "\n")
    print("Synced config pair_whitelist with selected/core pairs.")
    return 0


def _load_probation_state(path: Path) -> dict:
    if not path.exists():
        return {"pairs": {}}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {"pairs": {}}
    if not isinstance(payload, dict):
        return {"pairs": {}}
    pairs = payload.get("pairs")
    if not isinstance(pairs, dict):
        payload["pairs"] = {}
    return payload


def _write_probation_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _prune_probation_pairs(payload: dict) -> tuple[dict, list[str]]:
    now = datetime.now(timezone.utc)
    active: list[str] = []
    pairs = payload.get("pairs", {})
    if not isinstance(pairs, dict):
        payload["pairs"] = {}
        return payload, active
    to_delete: list[str] = []
    for pair, meta in pairs.items():
        if not isinstance(meta, dict):
            to_delete.append(pair)
            continue
        expires_at_raw = str(meta.get("expires_at", "") or "").strip()
        if not expires_at_raw:
            to_delete.append(pair)
            continue
        expires_at = _parse_utc(expires_at_raw)
        if expires_at is None:
            to_delete.append(pair)
            continue
        if expires_at <= now:
            to_delete.append(pair)
            continue
        active.append(str(pair).strip().upper())
    for pair in to_delete:
        pairs.pop(pair, None)
    return payload, sorted(set(active))


def probation_active_pairs(args: argparse.Namespace) -> int:
    payload = _load_probation_state(args.state_path)
    payload, active = _prune_probation_pairs(payload)
    _write_probation_state(args.state_path, payload)
    print(" ".join(active))
    return 0


def probation_add(args: argparse.Namespace) -> int:
    payload = _load_probation_state(args.state_path)
    payload, _ = _prune_probation_pairs(payload)
    pairs = payload.setdefault("pairs", {})
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=max(1.0, float(args.hours)))
    for pair in parse_pairs(args.pairs):
        pairs[pair] = {
            "reason": str(args.reason or "").strip() or "manual_probation",
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
    _write_probation_state(args.state_path, payload)
    return 0


def _strong_momentum_recovery(item: dict) -> bool:
    price = float(item.get("price") or 0.0)
    ema20 = float(item.get("ema_20") or 0.0)
    ema50 = float(item.get("ema_50") or 0.0)
    rsi = float(item.get("rsi_14") or 0.0)
    adx = float(item.get("adx_14") or 0.0)
    atr_pct = float(item.get("atr_pct") or 0.0)
    vol_z = float(item.get("volume_zscore") or 0.0)
    trend_4h = str(item.get("trend_4h", "")).strip().lower()
    market_structure = str(item.get("market_structure", "")).strip().lower()
    return (
        price > 0.0
        and price >= ema20 >= ema50 > 0.0
        and 55.0 <= rsi <= 68.0
        and adx >= 28.0
        and atr_pct >= 1.25
        and vol_z >= 0.0
        and trend_4h == "bullish"
        and market_structure in {"higher_highs", "trend"}
    )


def _benchmark_positive_context(market_context: dict | None) -> bool:
    if not isinstance(market_context, dict):
        return False
    broad_move = str(market_context.get("broad_move", "")).strip().lower()
    btc_change = float(market_context.get("btc_change_pct") or 0.0)
    eth_change = float(market_context.get("eth_change_pct") or 0.0)
    alt_above = float(market_context.get("alt_above_ema20_ratio") or 0.0)
    alt_momentum = float(market_context.get("alt_momentum_ratio") or 0.0)
    overextended = bool(market_context.get("overextended"))
    return (
        broad_move == "risk_on"
        and btc_change >= 0.4
        and eth_change >= 0.4
        and alt_above >= 0.58
        and alt_momentum >= 0.45
        and not overextended
    )


def _candidate_recovery_signal(item: dict, market_context: dict | None) -> tuple[bool, bool]:
    strong_momentum = _strong_momentum_recovery(item)
    benchmark_positive = _benchmark_positive_context(market_context)
    return strong_momentum, strong_momentum and benchmark_positive


def apply_probation_to_metrics(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    state = _load_probation_state(args.state_path)
    state, active_before = _prune_probation_pairs(state)
    pairs = state.setdefault("pairs", {})
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=max(1.0, float(args.hours)))
    market_context = payload.get("market_context", {}) if isinstance(payload, dict) else {}
    shortened_now: list[str] = []
    released_now: list[str] = []

    for item in payload.get("candidates", []):
        if not isinstance(item, dict):
            continue
        pair = str(item.get("pair", "")).strip().upper()
        if not pair:
            continue
        closed_trades = int(item.get("recent_closed_trades") or 0)
        avg_profit_pct = float(item.get("recent_avg_profit_pct") or 0.0)
        net_profit_pct = float(item.get("recent_net_profit_pct") or 0.0)
        win_rate = float(item.get("recent_win_rate") or 0.0)
        if closed_trades < 4:
            continue
        if avg_profit_pct > -0.15 and net_profit_pct > -0.75:
            continue
        if win_rate > 0.4 and avg_profit_pct > -0.25:
            continue
        pairs[pair] = {
            "reason": "auto_negative_expectancy",
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }

    recovery_shortened_expires_at = now + timedelta(hours=12)
    for item in payload.get("candidates", []):
        if not isinstance(item, dict):
            continue
        pair = str(item.get("pair", "")).strip().upper()
        if not pair or pair not in pairs:
            continue
        strong_momentum, allow_reentry = _candidate_recovery_signal(item, market_context)
        if allow_reentry:
            pairs.pop(pair, None)
            released_now.append(pair)
            continue
        if not strong_momentum:
            continue
        current_expires_at = _parse_utc(str(pairs.get(pair, {}).get("expires_at", "")))
        if current_expires_at is None or recovery_shortened_expires_at >= current_expires_at:
            continue
        meta = dict(pairs.get(pair, {}))
        meta["expires_at"] = recovery_shortened_expires_at.isoformat()
        meta["recovery_reviewed_at"] = now.isoformat()
        meta["reason"] = "recovery_watch"
        pairs[pair] = meta
        shortened_now.append(pair)

    state, active = _prune_probation_pairs(state)
    _write_probation_state(args.state_path, state)

    active_set = set(active)
    filtered_candidates = []
    skipped = list(payload.get("skipped", []) or [])
    for item in payload.get("candidates", []):
        if not isinstance(item, dict):
            continue
        pair = str(item.get("pair", "")).strip().upper()
        if pair and pair in active_set:
            skipped.append({"pair": pair, "reason": "pair_probation_active"})
            continue
        filtered_candidates.append(item)
    payload["candidates"] = filtered_candidates
    payload["skipped"] = skipped
    notes = list(payload.get("discovery_notes", []) or [])
    added_now = sorted(set(active) - set(active_before))
    if added_now:
        notes.append(f"auto_probation_added:{' '.join(added_now)}")
    if shortened_now:
        notes.append(f"probation_shortened:{' '.join(sorted(set(shortened_now)))}")
    if released_now:
        notes.append(f"probation_released_early:{' '.join(sorted(set(released_now)))}")
    if active:
        notes.append(f"probation_filtered:{' '.join(active)}")
    payload["discovery_notes"] = notes
    print(json.dumps(payload, separators=(",", ":")))
    return 0
