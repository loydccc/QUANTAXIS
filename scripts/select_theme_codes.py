#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Select A-share codes for "hot themes" using AkShare (Eastmoney concept boards).

This solves the mapping problem: theme keywords (AI/机器人/航天/电力/变压器/半导体/存储等)
-> a concrete list of stock codes.

Usage:
  python /app/scripts/select_theme_codes.py --keywords AI,机器人,航天,军工,电力,变压器,半导体,存储 --top 5
  python /app/scripts/select_theme_codes.py --boards 人工智能,机器人概念 --print-codes

Outputs:
- Prints matched boards + sizes
- With --print-codes, prints a comma-separated 6-digit code list to stdout (for piping)
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import List, Set


def norm(s: str) -> str:
    return re.sub(r"\s+", "", s or "").lower()


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keywords", default="", help="comma-separated keywords, e.g. AI,机器人,航天,军工,电力")
    ap.add_argument("--boards", default="", help="comma-separated exact/partial board names to include")
    ap.add_argument("--top", type=int, default=10, help="max number of matched boards to use")
    ap.add_argument("--min-size", type=int, default=5, help="ignore boards smaller than this")
    ap.add_argument("--print-codes", action="store_true", help="print comma-separated 6-digit codes only")
    args = ap.parse_args(argv)

    try:
        import akshare as ak
    except Exception as e:
        print(f"ERROR: akshare not installed: {e}", file=sys.stderr)
        return 2

    kw = [k.strip() for k in args.keywords.split(",") if k.strip()]
    board_filters = [b.strip() for b in args.boards.split(",") if b.strip()]

    boards = ak.stock_board_concept_name_em()
    # expected columns: 板块名称
    name_col = None
    for c in boards.columns:
        if "板块" in c and "名称" in c:
            name_col = c
            break
    if not name_col:
        name_col = boards.columns[0]

    matched = []
    for name in boards[name_col].tolist():
        n0 = str(name)
        n = norm(n0)
        ok = False
        if board_filters:
            for b in board_filters:
                if norm(b) in n:
                    ok = True
                    break
        if kw:
            for k in kw:
                if norm(k) in n:
                    ok = True
                    break
        if ok:
            matched.append(n0)

    # de-dup keep order
    seen = set()
    matched2 = []
    for m in matched:
        if m not in seen:
            matched2.append(m)
            seen.add(m)

    matched2 = matched2[: max(0, args.top)]

    all_codes: Set[str] = set()
    board_sizes = []

    for bname in matched2:
        cons = ak.stock_board_concept_cons_em(symbol=bname)
        # expected columns contain code like "代码"
        code_col = None
        for c in cons.columns:
            if c in ("代码", "股票代码"):
                code_col = c
                break
        if not code_col:
            code_col = cons.columns[0]

        codes = []
        for x in cons[code_col].tolist():
            s = str(x).strip()
            if re.fullmatch(r"\d{6}", s):
                codes.append(s)
        if len(codes) < args.min_size:
            continue

        board_sizes.append((bname, len(codes)))
        all_codes.update(codes)

    if args.print_codes:
        print(",".join(sorted(all_codes)))
        return 0

    print("Matched boards:")
    for b, n in board_sizes:
        print(f"- {b}: {n}")
    print(f"Total unique codes: {len(all_codes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
