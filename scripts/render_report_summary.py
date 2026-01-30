#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render a human-readable summary.md for a report directory.

Inputs:
- metrics.json in report dir

Outputs:
- summary.md

This is intentionally lightweight so it can be used by CLI and later by API.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


def fmt_pct(x: Any) -> str:
    try:
        return f"{float(x)*100:.2f}%"
    except Exception:
        return str(x)


def fmt_num(x: Any) -> str:
    try:
        return f"{float(x):.4f}"
    except Exception:
        return str(x)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="path to report directory (contains metrics.json)")
    args = ap.parse_args()

    rpt = Path(args.report)
    metrics_path = rpt / "metrics.json"
    if not metrics_path.exists():
        raise SystemExit(f"missing {metrics_path}")

    m: Dict[str, Any] = json.loads(metrics_path.read_text(encoding="utf-8"))

    lines = []
    lines.append(f"# Report: {rpt.name}\n")

    lines.append("## Strategy\n")
    lines.append(f"- strategy: `{m.get('strategy')}`")
    lines.append(f"- theme: `{m.get('theme')}`")
    lines.append(f"- date range (effective): {m.get('start_effective')} .. {m.get('end_effective')}")
    lines.append(f"- bars: {m.get('bars')}")

    lines.append("\n## Performance (net of costs)\n")
    lines.append(f"- final equity: {fmt_num(m.get('final_equity'))}")
    lines.append(f"- CAGR: {fmt_pct(m.get('cagr'))}")
    lines.append(f"- vol (ann.): {fmt_pct(m.get('vol'))}")
    lines.append(f"- Sharpe: {fmt_num(m.get('sharpe'))}")
    lines.append(f"- max drawdown: {fmt_pct(m.get('max_drawdown'))}")

    lines.append("\n## Turnover / Costs\n")
    lines.append(f"- cost_bps (one-way): {m.get('cost_bps')}")
    lines.append(f"- avg daily turnover: {fmt_pct(m.get('avg_daily_turnover'))}")
    lines.append(f"- annual turnover (approx): {fmt_num(m.get('annual_turnover'))}")

    lines.append("\n## Universe\n")
    lines.append(f"- universe_size_raw: {m.get('universe_size_raw')}")
    lines.append(f"- universe_size_used: {m.get('universe_size')}")
    lines.append(f"- min_bars filter: {m.get('min_bars')}")
    dropped = m.get("universe_dropped") or []
    if dropped:
        lines.append(f"- dropped: {', '.join(dropped)}")
    lines.append(f"- universe_fingerprint: `{m.get('universe_fingerprint')}`")

    da = m.get("data_audit") or {}
    if da:
        lines.append("\n## Data audit\n")
        lines.append(f"- bars_min: {da.get('bars_min')}  |  bars_max: {da.get('bars_max')}")
        lines.append(f"- panel_missing_ratio: {fmt_pct(da.get('panel_missing_ratio'))}")

    lines.append("\n## Data source\n")
    data = m.get("data") or {}
    lines.append(f"- collection: `{data.get('collection')}`")
    lines.append(f"- price field: `{data.get('price')}`")
    lines.append(f"- adjustment: `{data.get('adjustment')}`")

    lines.append("\n## Artifacts\n")
    lines.append("- `metrics.json`\n- `equity.csv`\n- `positions.csv`")

    (rpt / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
