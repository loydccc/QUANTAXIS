#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--report', required=True)
    args = ap.parse_args()

    rpt = Path(args.report)
    m = json.loads((rpt / 'metrics.json').read_text(encoding='utf-8'))

    lines = []
    lines.append(f"# Factor evaluation summary")
    lines.append("")
    lines.append(f"- theme: **{m.get('theme')}**")
    lines.append(f"- horizon: **{m.get('horizon')}** days")
    lines.append(f"- start/end: **{m.get('start')}** → **{m.get('end')}**")
    lines.append("")

    lines.append("## IC / Spread")
    lines.append("")
    lines.append("|factor|mean IC (pearson)|IR (pearson)|mean IC (spearman)|IR (spearman)|avg q-spread (top-bottom)|n|")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")

    for fc, v in (m.get('factors') or {}).items():
        icp = (v.get('ic_pearson') or {})
        ics = (v.get('ic_spearman') or {})
        mean_p = icp.get('mean')
        ir_p = icp.get('ir')
        mean_s = ics.get('mean')
        ir_s = ics.get('ir')
        # n: prefer pearson n then spearman n
        n = icp.get('n') if icp.get('n') is not None else ics.get('n')
        spread = v.get('avg_qspread')
        def fmt(x):
            try:
                return f"{float(x):.4f}"
            except Exception:
                return "nan"
        lines.append(f"|{fc}|{fmt(mean_p)}|{fmt(ir_p)}|{fmt(mean_s)}|{fmt(ir_s)}|{fmt(spread)}|{fmt(n)}|")

    (rpt / 'summary.md').write_text("\n".join(lines) + "\n", encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
