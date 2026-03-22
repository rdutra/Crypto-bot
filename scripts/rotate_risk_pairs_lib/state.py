from __future__ import annotations

import argparse
import json
import sys

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
