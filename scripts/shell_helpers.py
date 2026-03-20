#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def read_env_value(env_path: Path, key: str) -> int:
    if not env_path.exists():
        return 0
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key.strip() != key:
            continue
        print(value.strip().strip('"').strip("'"))
        return 0
    return 0


def validate_config_mode(config_path: Path, expected_mode: str) -> int:
    if not config_path.exists():
        print(f"Missing config file: {config_path}", file=sys.stderr)
        return 1
    cfg = json.loads(config_path.read_text())
    dry_run = bool(cfg.get("dry_run", False))
    if expected_mode == "dry-run" and not dry_run:
        print(
            "Refusing to run: config has dry_run=false. Use run-live-watch.sh for real trading.",
            file=sys.stderr,
        )
        return 1
    if expected_mode == "live" and dry_run:
        print(
            "Refusing to run: config has dry_run=true. Set dry_run=false for live trading.",
            file=sys.stderr,
        )
        return 1
    return 0


def summarize_rotation_log(log_path: Path) -> int:
    if not log_path.exists():
        return 0
    lines = [line for line in log_path.read_text().splitlines() if line.strip()]
    if not lines:
        return 0
    last = json.loads(lines[-1])
    selected = " ".join(last.get("selected_pairs", [])) or "none"
    source = last.get("source", "unknown")
    reason = last.get("reason", "n/a")
    print(f"Rotation summary: source={source} reason={reason} selected={selected}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    env_value = subparsers.add_parser("env-value")
    env_value.add_argument("env_path", type=Path)
    env_value.add_argument("key")

    validate = subparsers.add_parser("validate-config-mode")
    validate.add_argument("config_path", type=Path)
    validate.add_argument("expected_mode", choices={"dry-run", "live"})

    summarize = subparsers.add_parser("summarize-rotation-log")
    summarize.add_argument("log_path", type=Path)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "env-value":
        return read_env_value(args.env_path, args.key)
    if args.command == "validate-config-mode":
        return validate_config_mode(args.config_path, args.expected_mode)
    if args.command == "summarize-rotation-log":
        return summarize_rotation_log(args.log_path)
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
