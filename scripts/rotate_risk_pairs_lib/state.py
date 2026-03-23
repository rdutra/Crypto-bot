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
        try:
            expires_at = datetime.fromisoformat(expires_at_raw)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            else:
                expires_at = expires_at.astimezone(timezone.utc)
        except ValueError:
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


def apply_probation_to_metrics(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    state = _load_probation_state(args.state_path)
    state, active_before = _prune_probation_pairs(state)
    pairs = state.setdefault("pairs", {})
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=max(1.0, float(args.hours)))

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
    if active:
        notes.append(f"probation_filtered:{' '.join(active)}")
    payload["discovery_notes"] = notes
    print(json.dumps(payload, separators=(",", ":")))
    return 0
