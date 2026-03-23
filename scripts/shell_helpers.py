#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _parse_pairs(raw: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for part in str(raw or "").replace(",", " ").split():
        pair = part.strip().upper()
        if not pair or pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return out


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


def current_whitelist_pairs(config_path: Path) -> int:
    if not config_path.exists():
        return 0
    try:
        payload = json.loads(config_path.read_text())
    except Exception:
        return 0
    exchange = payload.get("exchange", {})
    if not isinstance(exchange, dict):
        return 0
    print(" ".join(_parse_pairs(" ".join(str(item) for item in exchange.get("pair_whitelist", [])))))
    return 0


def recent_rotation_pairs(log_path: Path, max_entries: int, max_pairs: int) -> int:
    if not log_path.exists():
        return 0
    try:
        lines = [line for line in log_path.read_text().splitlines() if line.strip()]
    except Exception:
        return 0
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in reversed(lines[-max_entries:]):
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        for pair in payload.get("selected_pairs", []):
            normalized = str(pair).strip().upper()
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
        for row in payload.get("decisions", []):
            if not isinstance(row, dict):
                continue
            normalized = str(row.get("pair", "")).strip().upper()
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
        for row in payload.get("skipped", []):
            if not isinstance(row, dict):
                continue
            normalized = str(row.get("pair", "")).strip().upper()
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
        if len(ordered) >= max_pairs:
            break
    print(" ".join(ordered[:max_pairs]))
    return 0


def unique_pairs(pairs: list[str]) -> int:
    print(" ".join(_parse_pairs(" ".join(pairs))))
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

    whitelist = subparsers.add_parser("current-whitelist-pairs")
    whitelist.add_argument("config_path", type=Path)

    recent_rotation = subparsers.add_parser("recent-rotation-pairs")
    recent_rotation.add_argument("log_path", type=Path)
    recent_rotation.add_argument("max_entries", type=int)
    recent_rotation.add_argument("max_pairs", type=int)

    unique = subparsers.add_parser("unique-pairs")
    unique.add_argument("pairs", nargs="*")

    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "env-value":
        return read_env_value(args.env_path, args.key)
    if args.command == "validate-config-mode":
        return validate_config_mode(args.config_path, args.expected_mode)
    if args.command == "summarize-rotation-log":
        return summarize_rotation_log(args.log_path)
    if args.command == "current-whitelist-pairs":
        return current_whitelist_pairs(args.config_path)
    if args.command == "recent-rotation-pairs":
        return recent_rotation_pairs(args.log_path, args.max_entries, args.max_pairs)
    if args.command == "unique-pairs":
        return unique_pairs(args.pairs)
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
