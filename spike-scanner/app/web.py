from aiohttp import web

from app.storage import PredictionStore


def _esc(value) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _fmt_pct(value) -> str:
    if value is None:
        return ""
    return f"{float(value):.2f}%"


def _fmt_num(value, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


def _fmt_llm_allowed(value) -> str:
    if value is None:
        return "unknown"
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if parsed == 1:
        return "allowed"
    if parsed == 0:
        return "blocked"
    return "unknown"


async def dashboard(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    alerts = store.fetch_recent_alerts(limit=100)
    outcomes = store.fetch_recent_outcomes(limit=200)
    llm_evals = store.fetch_recent_llm_shadow_evals(limit=200)
    summary = store.fetch_horizon_summary()
    llm_summary = store.fetch_llm_outcome_summary()

    summary_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row.get('horizon_minutes', 0))}</td>"
                f"<td>{int(row.get('total', 0))}</td>"
                f"<td>{int(row.get('resolved', 0))}</td>"
                f"<td>{_fmt_pct(row.get('avg_return_pct'))}</td>"
                f"<td>{_fmt_pct((row.get('win_rate') or 0.0) * 100.0)}</td></tr>"
            )
            for row in summary
        ]
    )

    alert_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row['id'])}</td>"
                f"<td>{_esc(row.get('ts'))}</td>"
                f"<td>{_esc(row.get('symbol'))}</td>"
                f"<td>{_fmt_num(row.get('score'), 4)}</td>"
                f"<td>{_fmt_num(row.get('entry_price'), 8)}</td>"
                f"<td>{_esc(_fmt_llm_allowed(row.get('llm_allowed')))}</td>"
                f"<td>{_esc(row.get('llm_regime'))}</td>"
                f"<td>{_esc(row.get('llm_risk_level'))}</td>"
                f"<td>{_fmt_num(row.get('llm_confidence'), 4)}</td>"
                f"<td>{_esc(row.get('llm_reason'))}</td></tr>"
            )
            for row in alerts
        ]
    )

    outcome_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row['id'])}</td>"
                f"<td>{int(row.get('alert_id', 0))}</td>"
                f"<td>{_esc(row.get('symbol'))}</td>"
                f"<td>{int(row.get('horizon_minutes', 0))}</td>"
                f"<td>{_esc(row.get('status'))}</td>"
                f"<td>{_fmt_num(row.get('entry_price'), 8)}</td>"
                f"<td>{_fmt_num(row.get('observed_price'), 8)}</td>"
                f"<td>{_fmt_pct(row.get('return_pct'))}</td>"
                f"<td>{_esc(_fmt_llm_allowed(row.get('llm_allowed')))}</td>"
                f"<td>{_fmt_num(row.get('llm_confidence'), 4)}</td>"
                f"<td>{_esc(row.get('due_ts'))}</td>"
                f"<td>{_esc(row.get('resolved_ts'))}</td></tr>"
            )
            for row in outcomes
        ]
    )

    llm_summary_rows = "\n".join(
        [
            (
                f"<tr><td>{_fmt_llm_allowed(row.get('llm_allowed'))}</td>"
                f"<td>{int(row.get('horizon_minutes', 0))}</td>"
                f"<td>{int(row.get('resolved', 0))}</td>"
                f"<td>{_fmt_pct(row.get('avg_return_pct'))}</td>"
                f"<td>{_fmt_pct((row.get('win_rate') or 0.0) * 100.0)}</td></tr>"
            )
            for row in llm_summary
        ]
    )

    llm_eval_rows = "\n".join(
        [
            (
                f"<tr><td>{int(row.get('id', 0))}</td>"
                f"<td>{_esc(row.get('ts'))}</td>"
                f"<td>{_esc(row.get('symbol'))}</td>"
                f"<td>{_fmt_num(row.get('score'), 4)}</td>"
                f"<td>{_fmt_num(row.get('spread_pct'), 4)}</td>"
                f"<td>{'yes' if int(row.get('threshold_ok', 0)) == 1 else 'no'}</td>"
                f"<td>{'yes' if int(row.get('cooldown_ok', 0)) == 1 else 'no'}</td>"
                f"<td>{'yes' if int(row.get('eligible_alert', 0)) == 1 else 'no'}</td>"
                f"<td>{_esc(_fmt_llm_allowed(row.get('llm_allowed')))}</td>"
                f"<td>{_esc(row.get('llm_regime'))}</td>"
                f"<td>{_esc(row.get('llm_risk_level'))}</td>"
                f"<td>{_fmt_num(row.get('llm_confidence'), 4)}</td>"
                f"<td>{_esc(row.get('llm_reason'))}</td>"
                f"<td>{_esc(row.get('llm_latency_ms'))}</td></tr>"
            )
            for row in llm_evals
        ]
    )

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Spike Scanner Dashboard</title>
  <style>
    body {{ font-family: ui-sans-serif, -apple-system, Segoe UI, sans-serif; margin: 20px; color: #111; }}
    h1 {{ margin: 0 0 12px; }}
    h2 {{ margin-top: 28px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 8px; font-size: 13px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
    th {{ background: #f5f5f5; }}
    .wrap {{ overflow-x: auto; }}
    .muted {{ color: #666; font-size: 13px; }}
  </style>
</head>
<body>
  <h1>Spike Scanner Dashboard</h1>
  <div class="muted">Predictions and realized outcomes (1h/4h/24h/48h by default).</div>

  <h2>Summary By Horizon</h2>
  <div class="wrap">
    <table>
      <thead><tr><th>Horizon (min)</th><th>Total</th><th>Resolved</th><th>Avg Return</th><th>Win Rate</th></tr></thead>
      <tbody>{summary_rows}</tbody>
    </table>
  </div>

  <h2>Recent Predictions</h2>
  <div class="wrap">
    <table>
      <thead><tr><th>ID</th><th>Timestamp</th><th>Symbol</th><th>Score</th><th>Entry Price</th><th>LLM Allowed</th><th>LLM Regime</th><th>LLM Risk</th><th>LLM Conf</th><th>LLM Reason</th></tr></thead>
      <tbody>{alert_rows}</tbody>
    </table>
  </div>

  <h2>LLM Shadow Outcomes</h2>
  <div class="wrap">
    <table>
      <thead><tr><th>LLM Verdict</th><th>Horizon (min)</th><th>Resolved</th><th>Avg Return</th><th>Win Rate</th></tr></thead>
      <tbody>{llm_summary_rows}</tbody>
    </table>
  </div>

  <h2>Recent LLM Evaluations</h2>
  <div class="wrap">
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Timestamp</th><th>Symbol</th><th>Score</th><th>Spread %</th><th>Score OK</th><th>Cooldown OK</th><th>Alert Eligible</th>
          <th>LLM Verdict</th><th>LLM Regime</th><th>LLM Risk</th><th>LLM Conf</th><th>LLM Reason</th><th>Latency ms</th>
        </tr>
      </thead>
      <tbody>{llm_eval_rows}</tbody>
    </table>
  </div>

  <h2>Recent Outcomes</h2>
  <div class="wrap">
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Alert ID</th><th>Symbol</th><th>Horizon (min)</th><th>Status</th>
          <th>Entry</th><th>Observed</th><th>Return</th><th>LLM Allowed</th><th>LLM Conf</th><th>Due</th><th>Resolved</th>
        </tr>
      </thead>
      <tbody>{outcome_rows}</tbody>
    </table>
  </div>
</body>
</html>
"""
    return web.Response(text=html, content_type="text/html")


async def api_alerts(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    limit = int(request.query.get("limit", "200"))
    return web.json_response({"items": store.fetch_recent_alerts(limit=max(1, min(2000, limit)))})


async def api_outcomes(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    limit = int(request.query.get("limit", "300"))
    status = request.query.get("status")
    return web.json_response(
        {"items": store.fetch_recent_outcomes(limit=max(1, min(3000, limit)), status=status)}
    )


async def api_summary(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    return web.json_response(
        {
            "items": store.fetch_horizon_summary(),
            "llm_items": store.fetch_llm_outcome_summary(),
        }
    )


async def api_llm_evals(request: web.Request) -> web.Response:
    store: PredictionStore = request.app["store"]
    limit = int(request.query.get("limit", "200"))
    return web.json_response({"items": store.fetch_recent_llm_shadow_evals(limit=max(1, min(5000, limit)))})


def create_app(store: PredictionStore) -> web.Application:
    app = web.Application()
    app["store"] = store
    app.router.add_get("/", dashboard)
    app.router.add_get("/api/alerts", api_alerts)
    app.router.add_get("/api/outcomes", api_outcomes)
    app.router.add_get("/api/summary", api_summary)
    app.router.add_get("/api/llm-evals", api_llm_evals)
    return app
