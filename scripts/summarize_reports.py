#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Summarize the latest N reports into a comparison table (CSV + Markdown).

This is a productization step: stable, machine+human friendly artifacts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reports-dir", default="output/reports")
    ap.add_argument("--latest", type=int, default=3)
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--md", required=True, help="output Markdown path")
    args = ap.parse_args()

    rdir = Path(args.reports_dir)
    reports = [p for p in rdir.iterdir() if p.is_dir() and (p / "metrics.json").exists()]
    reports.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    reports = reports[: args.latest]

    rows: List[Dict[str, Any]] = []
    for rpt in reports:
        m = json.loads((rpt / "metrics.json").read_text(encoding="utf-8"))
        rows.append(
            {
                "run_id": rpt.name,
                "strategy": m.get("strategy"),
                "theme": m.get("theme"),
                "start": m.get("start_effective") or m.get("start"),
                "end": m.get("end_effective") or m.get("end"),
                "universe": m.get("universe_size"),
                "min_bars": m.get("min_bars"),
                "cost_bps": m.get("cost_bps"),
                "cagr": m.get("cagr"),
                "sharpe": m.get("sharpe"),
                "max_dd": m.get("max_drawdown"),
                "final_equity": m.get("final_equity"),
                "annual_turnover": m.get("annual_turnover"),
                "fingerprint": m.get("universe_fingerprint"),
            }
        )

    # Write CSV
    import csv

    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(rows[0].keys()) if rows else []
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Write Markdown (simple)
    out_md = Path(args.md)
    def fmt(x):
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x)

    lines = ["# Latest report comparison", "", "|strategy|cagr|sharpe|max_dd|final_equity|annual_turnover|universe|min_bars|cost_bps|run_id|", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in rows:
        lines.append(
            f"|{r['strategy']}|{fmt(r['cagr'])}|{fmt(r['sharpe'])}|{fmt(r['max_dd'])}|{fmt(r['final_equity'])}|{fmt(r['annual_turnover'])}|{r['universe']}|{r['min_bars']}|{r['cost_bps']}|{r['run_id']}|"
        )
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"WROTE {out_csv}")
    print(f"WROTE {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
