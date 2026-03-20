from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.parse import urlencode

from .common import fetch_json, finite


def _choose_info_item(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not items:
        return None
    tradable = [item for item in items if bool(item.get("is_binance_spot_tradable", False))]
    if tradable:
        return tradable[0]
    return items[0]



def apply_skill_prefilters(args: argparse.Namespace) -> int:
    payload = json.loads(args.metrics_json)
    survivors: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    notes = list(payload.get("prefilter_notes", []) or [])
    token_audit_block_levels = {
        part.strip().lower() for part in str(args.token_audit_block_levels or "").replace(",", " ").split() if part.strip()
    }

    for candidate in payload.get("candidates", []):
        pair = str(candidate.get("pair", "")).strip().upper()
        if not pair:
            continue
        base = pair.split("/", 1)[0]
        info_item = None
        reject_reasons: list[str] = []

        if args.use_token_info:
            params = {"symbol": base, "quote": pair.split("/", 1)[1]}
            try:
                info_payload = fetch_json(f"{args.bot_api_url.rstrip('/')}/skills/query-token-info?{urlencode(params)}")
                info_items = info_payload.get("items", []) if isinstance(info_payload, dict) else []
                info_items = [item for item in info_items if isinstance(item, dict)]
                info_item = _choose_info_item(info_items)
                if info_item is None:
                    notes.append(f"token_info_no_data_fail_open:{pair}")
                else:
                    liquidity = finite(info_item.get("liquidity_usd")) or 0.0
                    holders = int(finite(info_item.get("holders")) or 0)
                    top10_share = finite(info_item.get("top10_holder_share"))
                    tradable = bool(info_item.get("is_binance_spot_tradable", False))
                    if liquidity < args.min_liquidity_usd:
                        reject_reasons.append(f"liquidity_below:{liquidity:.2f}")
                    if holders < args.min_holders:
                        reject_reasons.append(f"holders_below:{holders}")
                    if top10_share is not None and top10_share > args.max_top10_share:
                        reject_reasons.append(f"top10_share_above:{top10_share:.4f}")
                    if args.require_spot_tradable and not tradable:
                        reject_reasons.append("not_binance_spot_tradable")
            except Exception as exc:
                if args.token_info_fail_open:
                    notes.append(f"token_info_fail_open:{pair}:{exc.__class__.__name__}")
                    info_item = None
                else:
                    reject_reasons.append(f"token_info_error:{exc.__class__.__name__}")

        if args.use_token_audit and not reject_reasons:
            params = {"symbol": base}
            try:
                audit_payload = fetch_json(f"{args.bot_api_url.rstrip('/')}/skills/query-token-audit?{urlencode(params)}")
                classification = str(audit_payload.get("classification", "")).strip().lower()
                if classification and classification in token_audit_block_levels:
                    reject_reasons.append(f"audit_blocked:{classification}")
                if not classification:
                    notes.append(f"token_audit_no_data_fail_open:{pair}")
            except Exception as exc:
                if args.token_audit_fail_open:
                    notes.append(f"token_audit_fail_open:{pair}:{exc.__class__.__name__}")
                else:
                    reject_reasons.append(f"token_audit_error:{exc.__class__.__name__}")

        if reject_reasons:
            stage = "token_info" if any(not reason.startswith("audit_") for reason in reject_reasons) else "token_audit"
            rejected.append({"pair": pair, "stage": stage, "reasons": reject_reasons})
            continue
        survivors.append(candidate)

    payload["candidates"] = survivors
    payload["prefilter_rejected"] = rejected
    payload["prefilter_notes"] = notes
    print(json.dumps(payload, separators=(",", ":")))
    return 0
