#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Walk-forward parameter scan on signal generation with transaction costs.

Scans production knobs:
- score_temp / max_name_weight / min_trade_weight
- impact_k / impact_alpha / impact_cost_budget_bps

Method:
- For each parameter combo, run weekly signals on rebalance dates in [start, end].
- Build daily close-to-close equity with T+1 execution and one-way cost_bps.
- Output ranked summary and per-combo metrics.

Outputs:
- output/reports/signal_param_scan/<label>/results.csv
- output/reports/signal_param_scan/<label>/results.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pymongo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from api import signals_impl as si
from ladder_audit_2022 import pick_rebalance_dates


OUTROOT = ROOT / "output" / "reports" / "signal_param_scan"
OUTROOT.mkdir(parents=True, exist_ok=True)


@dataclass
class Combo:
    score_temp: float
    max_name_weight: float
    min_trade_weight: float
    impact_k: float
    impact_alpha: float
    impact_cost_budget_bps: float

    @property
    def key(self) -> str:
        return (
            f"st={self.score_temp:.4f}"
            f"__mw={self.max_name_weight:.4f}"
            f"__mtw={self.min_trade_weight:.4f}"
            f"__ik={self.impact_k:.4f}"
            f"__ia={self.impact_alpha:.4f}"
            f"__ib={self.impact_cost_budget_bps:.4f}"
        )


def _parse_grid(s: str) -> List[float]:
    out = []
    for x in str(s).split(","):
        x = x.strip()
        if not x:
            continue
        out.append(float(x))
    if not out:
        raise ValueError("empty grid")
    return out


def _parse_combos(s: str) -> List[Combo]:
    out: List[Combo] = []
    raw = str(s or "").strip()
    if not raw:
        return out
    # format:
    # - legacy: score_temp:max_name_weight:min_trade_weight;...
    # - extended: score_temp:max_name_weight:min_trade_weight:impact_k:impact_alpha:impact_budget_bps;...
    for it in raw.split(";"):
        x = it.strip()
        if not x:
            continue
        parts = [p.strip() for p in x.split(":")]
        if len(parts) not in (3, 6):
            raise ValueError(f"bad combo item: {x!r}")
        if len(parts) == 3:
            out.append(Combo(float(parts[0]), float(parts[1]), float(parts[2]), 0.01, 0.70, 25.0))
        else:
            out.append(Combo(float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5])))
    return out


def _mongo_client() -> pymongo.MongoClient:
    host = os.getenv("MONGODB_HOST", "127.0.0.1")
    if host == "mongodb":
        host = "127.0.0.1"
    port = int(os.getenv("MONGODB_PORT", "27017"))
    dbn = os.getenv("MONGODB_DATABASE", "quantaxis")
    user = os.getenv("MONGODB_USER", "quantaxis")
    password = os.getenv("MONGODB_PASSWORD", "quantaxis")
    uri = f"mongodb://{user}:{password}@{host}:{port}/{dbn}?authSource=admin"
    return pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)


def _build_close_panel(db, codes: List[str], start: str, end: str) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    coll = db["stock_day"]
    start2 = start.replace("-", "")
    end2 = end.replace("-", "")
    start_i = int(start2)
    end_i = int(end2)
    q = {
        "code": {"$in": list(sorted(set(codes)))},
        "$or": [
            {"date": {"$gte": start, "$lte": end}},
            {"date": {"$gte": start2, "$lte": end2}},
            {"date": {"$gte": start_i, "$lte": end_i}},
        ],
    }
    proj = {"_id": 0, "code": 1, "date": 1, "close": 1}
    rows = list(coll.find(q, proj))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed", errors="coerce")
    df = df.dropna(subset=["date", "code"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    close = df.pivot(index="date", columns="code", values="close").sort_index()
    return close


def _weights_by_day(runs: list[dict], close_idx: pd.DatetimeIndex) -> pd.DataFrame:
    if not runs:
        return pd.DataFrame(index=close_idx)
    by_reb = {pd.to_datetime(r["rebalance_date"]): (r["positions"] or {}) for r in runs}
    reb_dates = sorted(by_reb.keys())
    cols = sorted({c for r in runs for c in (r["positions"] or {}).keys() if c != "CASH"})
    wdf = pd.DataFrame(0.0, index=close_idx, columns=cols)
    for i, d in enumerate(reb_dates):
        end_i = reb_dates[i + 1] if i + 1 < len(reb_dates) else close_idx.max() + pd.Timedelta(days=1)
        mask = (wdf.index >= d) & (wdf.index < end_i)
        pos = by_reb[d]
        for c, w in pos.items():
            if c == "CASH":
                continue
            if c in wdf.columns:
                wdf.loc[mask, c] = float(w)
    return wdf


def _perf_stats(net: pd.Series, turnover: pd.Series) -> Dict[str, float]:
    net = net.fillna(0.0)
    eq = (1.0 + net).cumprod()
    n = int(net.shape[0])
    ann = 252.0
    total_ret = float(eq.iloc[-1] - 1.0) if n > 0 else 0.0
    cagr = float(eq.iloc[-1] ** (ann / n) - 1.0) if n > 0 and float(eq.iloc[-1]) > 0 else 0.0
    vol_ann = float(net.std(ddof=1) * math.sqrt(ann)) if n > 1 else 0.0
    ret_ann = float(net.mean() * ann) if n > 0 else 0.0
    sharpe = float(ret_ann / vol_ann) if vol_ann > 1e-12 else 0.0
    dd = eq / eq.cummax() - 1.0
    max_dd = float(dd.min()) if n > 0 else 0.0
    avg_t = float(turnover.mean()) if n > 0 else 0.0
    return {
        "total_return": total_ret,
        "cagr": cagr,
        "ret_ann": ret_ann,
        "vol_ann": vol_ann,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "avg_daily_turnover": avg_t,
        "annual_turnover": float(avg_t * ann),
    }


def _run_combo(
    combo: Combo,
    combo_idx: int,
    rebs: List[str],
    *,
    start: str,
    end: str,
    theme: str,
    top_k: int,
    cost_bps: float,
    out_signals_dir: Path,
    execution_mode: str,
    aum_cny: float,
    adv_participation_max: float,
    impact_liq_floor: float,
    fee_bps: float,
    score_w_ret_20d: float | None,
    score_w_ret_10d: float | None,
    score_w_ret_5d: float | None,
    score_w_ma_60d: float | None,
    score_w_vol_20d: float | None,
    score_w_liq_20d: float | None,
) -> Dict[str, float]:
    out_signals_dir.mkdir(parents=True, exist_ok=True)
    original_signals_dir = si.SIGNALS_DIR
    si.SIGNALS_DIR = out_signals_dir
    try:
        runs: list[dict] = []
        held_codes: set[str] = {"510300"}

        for d in rebs:
            sid = f"scan_{combo_idx:03d}_{d.replace('-', '')}_{int(time.time() * 1000)}"
            cfg = {
                "strategy": "hybrid_baseline_weekly_topk",
                "theme": theme,
                "rebalance": "weekly",
                "top_k": int(top_k),
                "candidate_k": 100,
                "min_bars": 800,
                "liq_window": 20,
                "liq_min_ratio": 1.0,
                "hold_weeks": 2,
                "tranche_overlap": True,
                "ma_mode": "filter",
                "score_mode": "factor",
                "min_weight": 0.04,
                "hard_dist_252h_min": -0.4,
                "hard_downvol_q": 0.70,
                "fallback_asset": "510300",
                "start": "2019-01-01",
                "end": d,
                "health_date": d,
                "sealed_date": d,
                "data_version_id": f"ops_data_status@{d}",
                "manifest_sha256": "a" * 64,
                "weight_mode": "score",
                "score_temp": float(combo.score_temp),
                "max_name_weight": float(combo.max_name_weight),
                "min_trade_weight": float(combo.min_trade_weight),
                "execution_mode": str(execution_mode),
                "aum_cny": float(aum_cny),
                "adv_participation_max": float(adv_participation_max),
                "impact_k": float(combo.impact_k),
                "impact_alpha": float(combo.impact_alpha),
                "impact_liq_floor": float(impact_liq_floor),
                "impact_cost_budget_bps": float(combo.impact_cost_budget_bps),
                "fee_bps": float(fee_bps),
            }
            if score_w_ret_20d is not None:
                cfg["score_w_ret_20d"] = float(score_w_ret_20d)
            if score_w_ret_10d is not None:
                cfg["score_w_ret_10d"] = float(score_w_ret_10d)
            if score_w_ret_5d is not None:
                cfg["score_w_ret_5d"] = float(score_w_ret_5d)
            if score_w_ma_60d is not None:
                cfg["score_w_ma_60d"] = float(score_w_ma_60d)
            if score_w_vol_20d is not None:
                cfg["score_w_vol_20d"] = float(score_w_vol_20d)
            if score_w_liq_20d is not None:
                cfg["score_w_liq_20d"] = float(score_w_liq_20d)
            si.run_signal(sid, cfg)
            p = out_signals_dir / f"{sid}.json"
            if not p.exists():
                sp = out_signals_dir / f"{sid}.status.json"
                err = {}
                if sp.exists():
                    try:
                        err = json.loads(sp.read_text(encoding="utf-8"))
                    except Exception:
                        err = {"status_path": str(sp)}
                raise RuntimeError(f"signal json missing for {sid}: {err}")

            obj = json.loads(p.read_text(encoding="utf-8"))
            pos = {}
            for it in (obj.get("positions") or []):
                code = str(it.get("code", ""))
                if code.isdigit():
                    code = code.zfill(6)
                try:
                    w = float(it.get("weight", 0.0) or 0.0)
                except Exception:
                    w = 0.0
                if w > 0:
                    pos[code] = w
                    if code != "CASH":
                        held_codes.add(code)
            meta = obj.get("meta") or {}
            ex = meta.get("execution_realism") if isinstance(meta, dict) else None
            ex_present = isinstance(ex, dict)
            cost_est = (ex.get("cost_estimate") if ex_present and isinstance(ex.get("cost_estimate"), dict) else {}) or {}

            def _f(v, d=0.0):
                try:
                    return float(v)
                except Exception:
                    return float(d)

            def _i(v, d=0):
                try:
                    return int(v)
                except Exception:
                    return int(d)

            runs.append(
                {
                    "rebalance_date": str(obj.get("as_of_date") or d),
                    "positions": pos,
                    "positions_n": int(len(obj.get("positions") or [])),
                    "exec_meta_present": bool(ex_present),
                    "exec_enabled": bool(ex.get("enabled")) if ex_present else False,
                    "budget_scale": _f((ex.get("budget_scale") if ex_present else 1.0), 1.0),
                    "partial_fill_n": _i((ex.get("partial_fill_n") if ex_present else 0), 0),
                    "blocked_fill_n": _i((ex.get("blocked_fill_n") if ex_present else 0), 0),
                    "executed_turnover_2way": _f((ex.get("executed_turnover_2way") if ex_present else 0.0), 0.0),
                    "exec_total_cost_bps": _f(cost_est.get("total_cost_bps", 0.0), 0.0),
                }
            )

        cli = _mongo_client()
        dbn = os.getenv("MONGODB_DATABASE", "quantaxis")
        db = cli[dbn]
        close = _build_close_panel(db, sorted({c for c in held_codes if c != "CASH"}), start, end)
        if close.empty:
            raise RuntimeError("empty close panel for combo")

        w = _weights_by_day(runs, close.index)
        if w.empty:
            raise RuntimeError("empty weights panel for combo")

        w = w.reindex(close.index).ffill().fillna(0.0)
        w_eff = w.shift(1).fillna(0.0)
        dret = close.pct_change(fill_method=None).fillna(0.0)
        gross = (w_eff * dret).sum(axis=1)
        turnover = 0.5 * w_eff.diff().abs().sum(axis=1).fillna(0.0)
        cost = (float(cost_bps) / 10000.0) * turnover
        net = gross - cost
        metrics = _perf_stats(net, turnover)
        metrics["cost_ann"] = float(cost.mean() * 252.0)
        metrics["n_rebalances"] = int(len(rebs))
        metrics["mean_positions_n"] = float(pd.Series([r["positions_n"] for r in runs]).mean())
        metrics["min_positions_n"] = int(min(r["positions_n"] for r in runs))
        n_runs = max(1, int(len(runs)))
        n_meta = int(sum(1 for r in runs if bool(r.get("exec_meta_present"))))
        n_enabled = int(sum(1 for r in runs if bool(r.get("exec_enabled"))))
        n_budget_scaled = int(sum(1 for r in runs if float(r.get("budget_scale", 1.0)) < 0.999999))
        n_partial = int(sum(1 for r in runs if int(r.get("partial_fill_n", 0)) > 0))
        n_blocked = int(sum(1 for r in runs if int(r.get("blocked_fill_n", 0)) > 0))
        metrics["exec_meta_run_ratio"] = float(n_meta / n_runs)
        metrics["exec_enabled_run_ratio"] = float(n_enabled / n_runs)
        metrics["budget_scaled_run_ratio"] = float(n_budget_scaled / n_runs)
        metrics["partial_fill_run_ratio"] = float(n_partial / n_runs)
        metrics["blocked_fill_run_ratio"] = float(n_blocked / n_runs)
        metrics["avg_budget_scale"] = float(pd.Series([float(r.get("budget_scale", 1.0)) for r in runs]).mean())
        metrics["avg_partial_fill_n"] = float(pd.Series([float(r.get("partial_fill_n", 0)) for r in runs]).mean())
        metrics["avg_blocked_fill_n"] = float(pd.Series([float(r.get("blocked_fill_n", 0)) for r in runs]).mean())
        metrics["avg_exec_turnover_2way"] = float(pd.Series([float(r.get("executed_turnover_2way", 0.0)) for r in runs]).mean())
        metrics["avg_exec_est_total_cost_bps"] = float(pd.Series([float(r.get("exec_total_cost_bps", 0.0)) for r in runs]).mean())
        return metrics
    finally:
        si.SIGNALS_DIR = original_signals_dir


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--theme", default="a_ex_kcb_bse")
    ap.add_argument("--top-k", type=int, default=20)
    ap.add_argument("--cost-bps", type=float, default=10.0, help="One-way trading cost in bps")
    ap.add_argument("--score-temp-grid", default="0.35")
    ap.add_argument("--max-name-weight-grid", default="0.1666666667")
    ap.add_argument("--min-trade-weight-grid", default="0.005")
    ap.add_argument("--impact-k-grid", default="0.005,0.01")
    ap.add_argument("--impact-alpha-grid", default="0.6,0.8")
    ap.add_argument("--impact-budget-bps-grid", default="15,25")
    ap.add_argument("--execution-mode", default="shadow", choices=["naive", "realistic", "shadow"])
    ap.add_argument("--aum-cny", type=float, default=20_000_000.0)
    ap.add_argument("--adv-participation-max", type=float, default=0.10)
    ap.add_argument("--impact-liq-floor", type=float, default=1_000_000.0)
    ap.add_argument("--fee-bps", type=float, default=8.0)
    ap.add_argument("--score-w-ret-20d", type=float, default=None)
    ap.add_argument("--score-w-ret-10d", type=float, default=None)
    ap.add_argument("--score-w-ret-5d", type=float, default=None)
    ap.add_argument("--score-w-ma-60d", type=float, default=None)
    ap.add_argument("--score-w-vol-20d", type=float, default=None)
    ap.add_argument("--score-w-liq-20d", type=float, default=None)
    ap.add_argument(
        "--combos",
        default="",
        help=(
            "Explicit combos; overrides grids. "
            "Format: score_temp:max_name_weight:min_trade_weight:impact_k:impact_alpha:impact_budget_bps;..."
        ),
    )
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    # Host-run friendly Mongo defaults (avoid unresolved compose hostname "mongodb").
    os.environ.setdefault("MONGODB_HOST", "127.0.0.1")
    os.environ.setdefault("MONGODB_PORT", "27017")
    os.environ.setdefault("MONGODB_DATABASE", "quantaxis")
    os.environ.setdefault("MONGODB_USER", "quantaxis")
    os.environ.setdefault("MONGODB_PASSWORD", "quantaxis")
    if os.getenv("MONGODB_HOST") == "mongodb":
        os.environ["MONGODB_HOST"] = "127.0.0.1"

    label = str(args.label).strip() or f"scan_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}"
    outdir = OUTROOT / label
    outdir.mkdir(parents=True, exist_ok=True)

    rebs = pick_rebalance_dates(str(args.start), str(args.end), str(args.theme))
    if not rebs:
        raise RuntimeError("no rebalance dates in range")

    combos = _parse_combos(args.combos)
    if not combos:
        score_temp_grid = _parse_grid(args.score_temp_grid)
        max_name_weight_grid = _parse_grid(args.max_name_weight_grid)
        min_trade_weight_grid = _parse_grid(args.min_trade_weight_grid)
        impact_k_grid = _parse_grid(args.impact_k_grid)
        impact_alpha_grid = _parse_grid(args.impact_alpha_grid)
        impact_budget_grid = _parse_grid(args.impact_budget_bps_grid)
        combos = [
            Combo(st, mw, mtw, ik, ia, ib)
            for st, mw, mtw, ik, ia, ib in itertools.product(
                score_temp_grid,
                max_name_weight_grid,
                min_trade_weight_grid,
                impact_k_grid,
                impact_alpha_grid,
                impact_budget_grid,
            )
        ]

    rows = []
    for i, combo in enumerate(combos, start=1):
        print(json.dumps({"progress": f"{i}/{len(combos)}", "combo": combo.key}, ensure_ascii=False))
        combo_dir = outdir / "signals" / f"{i:03d}_{combo.key}"
        m = _run_combo(
            combo,
            i,
            rebs,
            start=str(args.start),
            end=str(args.end),
            theme=str(args.theme),
            top_k=int(args.top_k),
            cost_bps=float(args.cost_bps),
            out_signals_dir=combo_dir,
            execution_mode=str(args.execution_mode),
            aum_cny=float(args.aum_cny),
            adv_participation_max=float(args.adv_participation_max),
            impact_liq_floor=float(args.impact_liq_floor),
            fee_bps=float(args.fee_bps),
            score_w_ret_20d=args.score_w_ret_20d,
            score_w_ret_10d=args.score_w_ret_10d,
            score_w_ret_5d=args.score_w_ret_5d,
            score_w_ma_60d=args.score_w_ma_60d,
            score_w_vol_20d=args.score_w_vol_20d,
            score_w_liq_20d=args.score_w_liq_20d,
        )
        rows.append(
            {
                "combo_index": i,
                "score_temp": float(combo.score_temp),
                "max_name_weight": float(combo.max_name_weight),
                "min_trade_weight": float(combo.min_trade_weight),
                "impact_k": float(combo.impact_k),
                "impact_alpha": float(combo.impact_alpha),
                "impact_cost_budget_bps": float(combo.impact_cost_budget_bps),
                "score_w_ret_20d": (None if args.score_w_ret_20d is None else float(args.score_w_ret_20d)),
                "score_w_ret_10d": (None if args.score_w_ret_10d is None else float(args.score_w_ret_10d)),
                "score_w_ret_5d": (None if args.score_w_ret_5d is None else float(args.score_w_ret_5d)),
                "score_w_ma_60d": (None if args.score_w_ma_60d is None else float(args.score_w_ma_60d)),
                "score_w_vol_20d": (None if args.score_w_vol_20d is None else float(args.score_w_vol_20d)),
                "score_w_liq_20d": (None if args.score_w_liq_20d is None else float(args.score_w_liq_20d)),
                **m,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("empty results")

    # Rank by net Sharpe, then CAGR, then max drawdown (less negative is better).
    df = df.sort_values(["sharpe", "cagr", "max_dd"], ascending=[False, False, False]).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)

    best = df.iloc[0].to_dict()
    payload = {
        "period": {"start": str(args.start), "end": str(args.end)},
        "theme": str(args.theme),
        "top_k": int(args.top_k),
        "cost_bps": float(args.cost_bps),
        "execution_mode": str(args.execution_mode),
        "aum_cny": float(args.aum_cny),
        "adv_participation_max": float(args.adv_participation_max),
        "impact_liq_floor": float(args.impact_liq_floor),
        "fee_bps": float(args.fee_bps),
        "score_w_ret_20d": (None if args.score_w_ret_20d is None else float(args.score_w_ret_20d)),
        "score_w_ret_10d": (None if args.score_w_ret_10d is None else float(args.score_w_ret_10d)),
        "score_w_ret_5d": (None if args.score_w_ret_5d is None else float(args.score_w_ret_5d)),
        "score_w_ma_60d": (None if args.score_w_ma_60d is None else float(args.score_w_ma_60d)),
        "score_w_vol_20d": (None if args.score_w_vol_20d is None else float(args.score_w_vol_20d)),
        "score_w_liq_20d": (None if args.score_w_liq_20d is None else float(args.score_w_liq_20d)),
        "n_rebalance_dates": int(len(rebs)),
        "rebalance_dates": rebs,
        "n_combos": int(len(df)),
        "best": best,
        "results": df.to_dict(orient="records"),
    }

    (outdir / "results.csv").write_text(df.to_csv(index=False), encoding="utf-8")
    (outdir / "results.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "outdir": str(outdir),
                "best": {
                    "score_temp": best.get("score_temp"),
                    "max_name_weight": best.get("max_name_weight"),
                    "min_trade_weight": best.get("min_trade_weight"),
                    "impact_k": best.get("impact_k"),
                    "impact_alpha": best.get("impact_alpha"),
                    "impact_cost_budget_bps": best.get("impact_cost_budget_bps"),
                    "sharpe": best.get("sharpe"),
                    "cagr": best.get("cagr"),
                    "max_dd": best.get("max_dd"),
                    "annual_turnover": best.get("annual_turnover"),
                },
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
