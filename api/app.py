#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""QUANTAXIS product API (MVP).

Mode B (execute + read):
- Read endpoints serve artifacts under output/reports
- Run endpoint triggers existing CLI runner (scripts/run_from_cfg.py)

Security note: this is a local MVP. Do NOT expose to the public internet without auth.
"""

from __future__ import annotations

import json
import os
import subprocess
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import FileResponse, JSONResponse


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "output" / "reports"
RUNS_DIR = ROOT / "output" / "api_runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_DIR = ROOT / "output" / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="QUANTAXIS API", version="0.1.0")

# --- Security / hardening knobs (env) ---
API_TOKEN = os.getenv("QUANTAXIS_API_TOKEN", "").strip()
API_MAX_CONCURRENT = int(os.getenv("QUANTAXIS_API_MAX_CONCURRENT", "2"))
API_RUNS_PER_MIN = int(os.getenv("QUANTAXIS_API_RUNS_PER_MIN", "6"))
API_JOB_TIMEOUT_SEC = int(os.getenv("QUANTAXIS_API_JOB_TIMEOUT_SEC", "3600"))
API_LOG_TAIL = int(os.getenv("QUANTAXIS_API_LOG_TAIL", "2000"))
API_CFG_MAX_BYTES = int(os.getenv("QUANTAXIS_API_CFG_MAX_BYTES", "200000"))
API_CFG_MAX_DEPTH = int(os.getenv("QUANTAXIS_API_CFG_MAX_DEPTH", "12"))
API_INCLUDE_LOGS = os.getenv("QUANTAXIS_API_INCLUDE_LOGS", "").strip().lower() in {"1", "true", "yes"}

# In-memory concurrency + rate limit (good enough for local/one-process MVP)
_job_sem = threading.BoundedSemaphore(max(1, API_MAX_CONCURRENT))
_rl_lock = threading.Lock()
_rl_hits: Dict[str, Deque[float]] = defaultdict(deque)


def require_token(req: Request) -> None:
    """Require X-API-Key when QUANTAXIS_API_TOKEN is set."""
    if not API_TOKEN:
        return
    key = req.headers.get("x-api-key") or req.headers.get("X-API-Key")
    if key != API_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


def _rate_limit_run(req: Request) -> None:
    """Simple per-IP rate limit for /run (in-memory)."""
    if API_RUNS_PER_MIN <= 0:
        return
    ip = (req.client.host if req.client else "unknown")
    now = time.monotonic()
    window = 60.0
    with _rl_lock:
        dq = _rl_hits[ip]
        # drop old entries
        while dq and now - dq[0] > window:
            dq.popleft()
        if len(dq) >= API_RUNS_PER_MIN:
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        dq.append(now)


def _walk_depth(x: Any, depth: int = 0) -> int:
    if isinstance(x, dict) and x:
        return max(_walk_depth(v, depth + 1) for v in x.values())
    if isinstance(x, list) and x:
        return max(_walk_depth(v, depth + 1) for v in x)
    return depth


_strategy_re = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


def _validate_cfg(cfg: Dict[str, Any]) -> None:
    """Config sanity checks (MVP hardening).

    Not a full schema yet, but blocks common abuse:
    - over-large payloads
    - very deep nesting
    - pathological strings/keys
    - unexpected types for key fields
    """
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="config must be a JSON object")

    # serialized size guard (prevents huge payloads)
    try:
        raw = json.dumps(cfg, ensure_ascii=False)
    except Exception:
        raise HTTPException(status_code=400, detail="config must be JSON-serializable")
    if API_CFG_MAX_BYTES > 0 and len(raw.encode("utf-8")) > API_CFG_MAX_BYTES:
        raise HTTPException(status_code=400, detail="config too large")

    # depth guard (prevents deeply nested bombs)
    if API_CFG_MAX_DEPTH > 0 and _walk_depth(cfg) > API_CFG_MAX_DEPTH:
        raise HTTPException(status_code=400, detail="config too deeply nested")

    if len(cfg) > 300:
        raise HTTPException(status_code=400, detail="config has too many keys")

    if "strategy" not in cfg:
        raise HTTPException(status_code=400, detail="missing strategy")
    if not isinstance(cfg.get("strategy"), str) or not _strategy_re.match(cfg["strategy"]):
        raise HTTPException(status_code=400, detail="bad strategy")

    for k, v in cfg.items():
        if not isinstance(k, str):
            raise HTTPException(status_code=400, detail="config keys must be strings")
        if len(k) > 200:
            raise HTTPException(status_code=400, detail="bad config key")
        if k.startswith("$") or ".." in k or "/" in k or "\\" in k or "\x00" in k:
            raise HTTPException(status_code=400, detail="bad config key")
        if isinstance(v, str) and len(v) > 5000:
            raise HTTPException(status_code=400, detail=f"config value too long: {k}")
        if isinstance(v, list) and len(v) > 5000:
            raise HTTPException(status_code=400, detail=f"config list too long: {k}")

    # optional: forbid user-supplied workdir / command-like keys
    for forbidden in ("cmd", "command", "shell", "cwd", "workdir", "path"):
        if forbidden in cfg:
            raise HTTPException(status_code=400, detail=f"forbidden field: {forbidden}")


def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _redact_text(s: str) -> str:
    """Best-effort redaction for logs."""
    if not s:
        return s
    out = s
    if API_TOKEN:
        out = out.replace(API_TOKEN, "<REDACTED>")
    # common patterns
    out = re.sub(r"(?i)(x-api-key\s*[:=]\s*)([^\s]+)", r"\1<REDACTED>", out)
    out = re.sub(r"(?i)(token\s*[:=]\s*)([^\s]+)", r"\1<REDACTED>", out)
    out = re.sub(r"(?i)(password\s*[:=]\s*)([^\s]+)", r"\1<REDACTED>", out)
    return out


def run_job(job_id: str, cfg: Dict[str, Any]) -> None:
    """Run a backtest job and persist status/result under output/api_runs/<job_id>.json"""
    job_path = RUNS_DIR / f"{job_id}.json"
    job_path.write_text(
        json.dumps({"job_id": job_id, "status": "running", "started_at": int(time.time())}, ensure_ascii=False),
        encoding="utf-8",
    )

    cfg_path = RUNS_DIR / f"{job_id}.cfg.json"
    cfg_path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

    result_path = RUNS_DIR / f"{job_id}.result.json"

    try:
        # run_from_cfg prints result JSON on stdout; also writes --result file
        cmd = ["python3", "scripts/run_from_cfg.py", "--config", str(cfg_path), "--result", str(result_path)]
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=max(1, API_JOB_TIMEOUT_SEC),
        )
        rc = proc.returncode

        result_obj: Optional[Dict[str, Any]] = None
        if result_path.exists():
            result_obj = read_json(result_path)

        stdout_tail = (proc.stdout or "")[-API_LOG_TAIL:]
        stderr_tail = (proc.stderr or "")[-API_LOG_TAIL:]

        payload: Dict[str, Any] = {
            "job_id": job_id,
            "status": "succeeded" if rc == 0 else "failed",
            "finished_at": int(time.time()),
            "return_code": rc,
            "result": result_obj,
            # store tails for troubleshooting (optionally served by API)
            "stdout_tail": _redact_text(stdout_tail),
            "stderr_tail": _redact_text(stderr_tail),
        }
        job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    except subprocess.TimeoutExpired:
        payload = {
            "job_id": job_id,
            "status": "failed",
            "finished_at": int(time.time()),
            "error": f"timeout after {API_JOB_TIMEOUT_SEC}s",
        }
        job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    except Exception as e:
        payload = {
            "job_id": job_id,
            "status": "failed",
            "finished_at": int(time.time()),
            "error": repr(e),
        }
        job_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    finally:
        # release the concurrency slot acquired in /run
        try:
            _job_sem.release()
        except Exception:
            pass


@app.get("/")
def index():
    """Small landing page for humans."""
    base = "http://127.0.0.1:8000"
    return HTMLResponse(
        """
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>QUANTAXIS API</title>
  <style>
    body{font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin:32px; line-height:1.5}
    code{background:#f5f5f5; padding:2px 6px; border-radius:4px}
    a{color:#0b63ce; text-decoration:none}
    a:hover{text-decoration:underline}
  </style>
</head>
<body>
  <h1>QUANTAXIS API</h1>
  <p>Local MVP API for reading report artifacts and triggering backtest runs.</p>
  <ul>
    <li><a href=\"/docs\">/docs</a> (Swagger UI)</li>
    <li><a href=\"/redoc\">/redoc</a> (ReDoc)</li>
    <li><a href=\"/health\">/health</a></li>
    <li><code>GET /latest/manifest</code></li>
    <li><code>GET /reports/{run_id}/manifest</code></li>
    <li><code>GET /reports/{run_id}/file/{name}</code></li>
    <li><code>POST /run</code> (requires X-API-Key if token is set)</li>
    <li><code>GET /runs/{job_id}</code> (requires X-API-Key if token is set)</li>
    <li><code>POST /signals/run</code> (baseline weekly topK signals)</li>
    <li><code>GET /signals/{signal_id}</code></li>
    <li><code>GET /signals/{signal_id}.csv</code></li>
  </ul>
  <p>Tip: if you opened <code>""" + base + """</code> in a browser, 404 on <code>/</code> is now fixed.</p>
</body>
</html>
"""
    )


@app.get("/health")
def health():
    return {
        "ok": True,
        "ts": int(time.time()),
        "auth_required": bool(API_TOKEN),
        "limits": {
            "max_concurrent": API_MAX_CONCURRENT,
            "runs_per_min": API_RUNS_PER_MIN,
            "job_timeout_sec": API_JOB_TIMEOUT_SEC,
            "include_logs": API_INCLUDE_LOGS,
        },
    }


@app.get("/latest/manifest")
def latest_manifest():
    p = REPORTS_DIR / "latest_manifest.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="latest_manifest.json not found")
    return JSONResponse(read_json(p))


@app.get("/reports/{run_id}/manifest")
def report_manifest(run_id: str):
    p = REPORTS_DIR / run_id / "manifest.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="manifest.json not found")
    return JSONResponse(read_json(p))


@app.get("/reports/{run_id}/file/{name}")
def report_file(run_id: str, name: str):
    # allowlist simple filenames to reduce path traversal risk
    if "/" in name or ".." in name:
        raise HTTPException(status_code=400, detail="bad filename")
    p = REPORTS_DIR / run_id / name
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(str(p))


@app.post("/run")
def run(cfg: Dict[str, Any], background: BackgroundTasks, request: Request):
    require_token(request)
    _rate_limit_run(request)
    _validate_cfg(cfg)

    # concurrency guard
    acquired = _job_sem.acquire(blocking=False)
    if not acquired:
        raise HTTPException(status_code=429, detail="too many concurrent runs")

    job_id = uuid.uuid4().hex
    try:
        background.add_task(run_job, job_id, cfg)
    except Exception:
        # if BackgroundTasks fails for any reason, release the slot
        try:
            _job_sem.release()
        except Exception:
            pass
        raise

    return {"job_id": job_id, "status": "queued"}


@app.get("/runs/{job_id}")
def run_status(job_id: str, request: Request):
    require_token(request)
    p = RUNS_DIR / f"{job_id}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="job not found")
    obj = read_json(p)
    if not API_INCLUDE_LOGS:
        obj.pop("stdout_tail", None)
        obj.pop("stderr_tail", None)
    return JSONResponse(obj)


# -------------------------
# Signals API (Mode C MVP)
# -------------------------

_baseline_strategy_re = re.compile(r"^(xsec_momentum_weekly_topk|ts_ma_weekly|hybrid_baseline_weekly_topk)$")
_theme_re = re.compile(r"^[A-Za-z0-9_.-]{1,60}$")


def _validate_signal_cfg(cfg: Dict[str, Any]) -> None:
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="config must be a JSON object")

    # size/depth reuse
    _validate_cfg({"strategy": "demo", **{k: v for k, v in cfg.items() if k != "strategy"}})  # type: ignore[arg-type]

    strategy = cfg.get("strategy")
    theme = cfg.get("theme", "all")
    top_k = cfg.get("top_k", 10)
    rebalance = cfg.get("rebalance", "weekly")
    hold_weeks = cfg.get("hold_weeks", 2)
    tranche_overlap = cfg.get("tranche_overlap", True)
    liq_window = cfg.get("liq_window", 20)
    liq_min_ratio = cfg.get("liq_min_ratio", 1.0)
    ma_mode = cfg.get("ma_mode", "filter")

    if not isinstance(strategy, str) or not _baseline_strategy_re.match(strategy):
        raise HTTPException(status_code=400, detail="bad strategy")
    if not isinstance(theme, str) or not _theme_re.match(theme):
        raise HTTPException(status_code=400, detail="bad theme")
    if rebalance != "weekly":
        raise HTTPException(status_code=400, detail="only weekly rebalance supported in MVP")
    if not isinstance(top_k, int) or top_k <= 0 or top_k > 200:
        raise HTTPException(status_code=400, detail="bad top_k")
    if not isinstance(hold_weeks, int) or hold_weeks < 1 or hold_weeks > 8:
        raise HTTPException(status_code=400, detail="bad hold_weeks")
    if not isinstance(tranche_overlap, bool):
        raise HTTPException(status_code=400, detail="bad tranche_overlap")

    if not isinstance(liq_window, int) or liq_window < 0 or liq_window > 252:
        raise HTTPException(status_code=400, detail="bad liq_window")
    try:
        liq_min_ratio_f = float(liq_min_ratio)
    except Exception:
        raise HTTPException(status_code=400, detail="bad liq_min_ratio")
    if liq_min_ratio_f <= 0 or liq_min_ratio_f > 1.0:
        raise HTTPException(status_code=400, detail="bad liq_min_ratio")

    if ma_mode not in {"filter", "boost"}:
        raise HTTPException(status_code=400, detail="bad ma_mode (filter|boost)")


def _write_signal_csv(path: Path, positions: list[dict]) -> None:
    # minimal CSV (code, weight, rank, score)
    lines = ["code,weight,rank,score"]
    for p in positions:
        lines.append(f"{p['code']},{p['weight']},{p['rank']},{p['score']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _positions_to_portfolio(positions: list[dict]) -> Dict[str, float]:
    return {p["code"]: float(p["weight"]) for p in positions}


def _portfolio_to_positions(port: Dict[str, float], scores: Optional[Dict[str, float]] = None) -> list[dict]:
    # normalize
    s = sum(max(0.0, float(w)) for w in port.values())
    if s <= 0:
        return []
    items = [(c, float(w) / s) for c, w in port.items() if float(w) > 0]
    # sort by weight desc, then score desc, then code
    def sk(x):
        c, w = x
        sc = (scores or {}).get(c, 0.0)
        return (-w, -sc, c)

    items.sort(key=sk)
    out = []
    for i, (c, w) in enumerate(items, start=1):
        out.append({"code": c, "weight": round(w, 10), "rank": i, "score": round((scores or {}).get(c, 0.0), 6)})
    return out


def _extract_last_tranches_from_positions_csv(path: Path, n: int = 2) -> list[dict]:
    """Extract last N tranche snapshots from positions.csv.

    We detect rebalance-effective days by row changes; then infer rebalance_date as previous trading day.
    Returns list of dicts: {rebalance_date, effective_date, weights{code:weight}}
    """
    import pandas as pd

    df = pd.read_csv(path)
    if df.shape[0] < 2:
        raise RuntimeError("positions.csv too short")
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    # find rows where any position changed vs previous row
    changed = (df.diff().abs().sum(axis=1) > 1e-12)
    # keep only changes that lead to some non-zero exposure
    nonzero = (df.abs().sum(axis=1) > 0)
    eff_dates = [d for d in df.index[changed & nonzero]]
    if not eff_dates:
        # fallback to last non-zero day
        eff_dates = [df.index[nonzero][-1]]

    eff_dates = eff_dates[-n:]
    out = []
    for d in eff_dates[::-1]:
        # inferred rebalance date = previous available trading day
        idx = df.index.get_loc(d)
        reb = df.index[idx - 1] if isinstance(idx, int) and idx > 0 else d
        row = df.loc[d]
        weights = {c: float(row[c]) for c in df.columns if pd.notna(row[c]) and float(row[c]) > 0}
        out.append({"rebalance_date": str(reb.date()), "effective_date": str(d.date()), "weights": weights})
    return out


def _extract_latest_positions_from_positions_csv(path: Path) -> tuple[str, list[tuple[str, float]]]:
    """Return (as_of_date, [(code, weight>0), ...]) from the latest non-zero row."""
    import csv

    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    if not rows:
        raise RuntimeError("positions.csv is empty")

    last_row = None
    for r in reversed(rows):
        try:
            if any((c and float(c) != 0.0) for c in r[1:]):
                last_row = r
                break
        except Exception:
            continue
    if last_row is None:
        last_row = rows[-1]

    as_of_date = last_row[0]
    weights = []
    for i in range(1, len(header)):
        c = last_row[i]
        try:
            w = float(c) if c else 0.0
        except Exception:
            w = 0.0
        if w > 0:
            weights.append((header[i], w))

    weights.sort(key=lambda x: x[1], reverse=True)
    return as_of_date, weights


def _run_baseline_backtest_to_workdir(workdir: Path, cfg: Dict[str, Any], strategy: str) -> tuple[str, list[str], Dict[str, Any]]:
    """Run baseline backtest and return (as_of_date, picks_by_weight_desc, metrics_obj)."""
    theme = str(cfg.get("theme", "all"))
    top_k = int(cfg.get("top_k", 10))
    lookback = int(cfg.get("lookback", 60))
    ma = int(cfg.get("ma", 60))
    min_bars = int(cfg.get("min_bars", 800))
    liq_window = int(cfg.get("liq_window", 20))
    liq_min_ratio = float(cfg.get("liq_min_ratio", 1.0))
    cost_bps = float(cfg.get("cost_bps", 10.0))
    start = str(cfg.get("start", "2019-01-01"))
    end = str(cfg.get("end", "2099-12-31"))

    cmd = [
        "python3",
        "scripts/backtest_baseline.py",
        "--start",
        start,
        "--end",
        end,
        "--theme",
        theme,
        "--strategy",
        strategy,
        "--lookback",
        str(lookback),
        "--top",
        str(top_k),
        "--ma",
        str(ma),
        "--min-bars",
        str(min_bars),
        "--liq-window",
        str(liq_window),
        "--liq-min-ratio",
        str(liq_min_ratio),
        "--cost-bps",
        str(cost_bps),
        "--outdir",
        str(workdir),
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=max(1, API_JOB_TIMEOUT_SEC),
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "run failed")[-API_LOG_TAIL:])

    positions_csv = workdir / "positions.csv"
    metrics_json = workdir / "metrics.json"
    if not positions_csv.exists() or not metrics_json.exists():
        raise RuntimeError("missing baseline outputs (positions.csv/metrics.json)")

    as_of_date, weights = _extract_latest_positions_from_positions_csv(positions_csv)
    picks = [c for c, _w in weights]
    stats = read_json(metrics_json)
    return as_of_date, picks, stats


def run_signal(signal_id: str, cfg: Dict[str, Any]) -> None:
    """Generate a weekly topK signal as JSON+CSV under output/signals/."""
    status_path = SIGNALS_DIR / f"{signal_id}.status.json"
    status_path.write_text(
        json.dumps({"signal_id": signal_id, "status": "running", "started_at": int(time.time())}, ensure_ascii=False),
        encoding="utf-8",
    )

    workdir = SIGNALS_DIR / f"{signal_id}.work"
    workdir.mkdir(parents=True, exist_ok=True)

    strategy = str(cfg.get("strategy"))
    theme = str(cfg.get("theme", "all"))
    top_k = int(cfg.get("top_k", 10))
    lookback = int(cfg.get("lookback", 60))
    ma = int(cfg.get("ma", 60))
    min_bars = int(cfg.get("min_bars", 800))
    liq_window = int(cfg.get("liq_window", 20))
    liq_min_ratio = float(cfg.get("liq_min_ratio", 1.0))
    ma_mode = str(cfg.get("ma_mode", "filter"))
    hold_weeks = int(cfg.get("hold_weeks", 2))
    tranche_overlap = bool(cfg.get("tranche_overlap", True))

    try:
        if strategy == "hybrid_baseline_weekly_topk":
            # 1) momentum topK
            mom_dir = workdir / "mom"
            mom_dir.mkdir(parents=True, exist_ok=True)
            mom_date, mom_picks, mom_stats = _run_baseline_backtest_to_workdir(mom_dir, cfg, "xsec_momentum_weekly_topk")

            # 2) MA filter (acts like a breadth filter)
            ma_dir = workdir / "ma"
            ma_dir.mkdir(parents=True, exist_ok=True)
            ma_date, ma_picks, ma_stats = _run_baseline_backtest_to_workdir(ma_dir, cfg, "ts_ma_weekly")

            # tranche overlap extraction
            import pandas as pd

            mom_tr = _extract_last_tranches_from_positions_csv(mom_dir / "positions.csv", n=max(hold_weeks, 1))
            ma_tr = _extract_last_tranches_from_positions_csv(ma_dir / "positions.csv", n=max(hold_weeks, 1))

            # build per-tranche pick lists using latest snapshots (aligned by order)
            tranche_objs = []
            final_port: Dict[str, float] = {}
            final_scores: Dict[str, float] = {}

            n_tr = 1 if not tranche_overlap or hold_weeks <= 1 else min(2, hold_weeks)
            scale = 1.0 / n_tr

            for t in range(n_tr):
                mom_w = mom_tr[t]["weights"] if t < len(mom_tr) else {}
                ma_w = ma_tr[t]["weights"] if t < len(ma_tr) else {}
                # momentum picks: take top_k by weight (they are equal, but keep stable)
                mom_sorted = sorted(mom_w.items(), key=lambda x: (-x[1], x[0]))
                mom_top = [c for c, _w in mom_sorted][:top_k]
                ma_set = set([c for c, w in ma_w.items() if w > 0])

                scores: Dict[str, float] = {}
                mom_rank: Dict[str, int] = {}
                for i, code in enumerate(mom_top, start=1):
                    mom_rank[code] = i
                    scores[code] = float(top_k - i + 1)
                if ma_mode == "boost":
                    for code in ma_set:
                        scores[code] = scores.get(code, 0.0) + 1.0

                if ma_mode == "filter":
                    candidates = set(mom_top) & ma_set
                    if len(candidates) < max(3, min(5, top_k)):
                        candidates = set(ma_set)
                else:
                    candidates = set(mom_top) | ma_set

                def sort_key(code: str):
                    return (-scores.get(code, 0.0), mom_rank.get(code, 10**9), code)

                picks = sorted(candidates, key=sort_key)[:top_k]

                # tranche equal weights
                tranche_port = {c: (1.0 / len(picks) if picks else 0.0) for c in picks}

                # merge to final portfolio
                for c, w in tranche_port.items():
                    final_port[c] = final_port.get(c, 0.0) + scale * w
                    final_scores[c] = max(final_scores.get(c, 0.0), scores.get(c, 0.0))

                tranche_objs.append(
                    {
                        "rebalance_date": mom_tr[t]["rebalance_date"],
                        "effective_date": mom_tr[t]["effective_date"],
                        "picks": picks,
                    }
                )

            positions = _portfolio_to_positions(final_port, scores=final_scores)
            as_of_date = tranche_objs[0]["rebalance_date"] if tranche_objs else (mom_date or ma_date)

            # meta: keep both components for debugging
            stats = mom_stats
            meta_extra = {
                "ma_mode": ma_mode,
                "hold_weeks": hold_weeks,
                "tranche_overlap": tranche_overlap,
                "tranches": tranche_objs,
                "component_momentum_as_of": mom_date,
                "component_ma_as_of": ma_date,
                "component_momentum_topk": mom_picks[:top_k],
                "component_ma_long": sorted(list(set(ma_picks)))[:200],
            }

        else:
            # baseline single-strategy signal
            base_date, base_picks, stats = _run_baseline_backtest_to_workdir(workdir, cfg, strategy)
            # tranche overlap for baseline
            tr = _extract_last_tranches_from_positions_csv(workdir / "positions.csv", n=max(hold_weeks, 1))
            n_tr = 1 if not tranche_overlap or hold_weeks <= 1 else min(2, hold_weeks)
            scale = 1.0 / n_tr
            final_port: Dict[str, float] = {}
            tranche_objs = []
            for t in range(n_tr):
                wmap = tr[t]["weights"] if t < len(tr) else {}
                picks = [c for c, _w in sorted(wmap.items(), key=lambda x: (-x[1], x[0]))][:top_k]
                tranche_port = {c: (1.0 / len(picks) if picks else 0.0) for c in picks}
                for c, w in tranche_port.items():
                    final_port[c] = final_port.get(c, 0.0) + scale * w
                tranche_objs.append({"rebalance_date": tr[t]["rebalance_date"], "effective_date": tr[t]["effective_date"], "picks": picks})

            positions = _portfolio_to_positions(final_port)
            as_of_date = tranche_objs[0]["rebalance_date"] if tranche_objs else base_date
            meta_extra = {"hold_weeks": hold_weeks, "tranche_overlap": tranche_overlap, "tranches": tranche_objs}
        signal_obj: Dict[str, Any] = {
            "signal_id": signal_id,
            "status": "succeeded",
            "generated_at": int(time.time()),
            "as_of_date": as_of_date,
            "strategy": strategy,
            "rebalance": "weekly",
            "theme": theme,
            "top_k": top_k,
            "positions": positions,
            "meta": {
                "universe_fingerprint": stats.get("universe_fingerprint"),
                "universe_size": stats.get("universe_size"),
                "min_bars": min_bars,
                "liq_window": liq_window,
                "liq_min_ratio": liq_min_ratio,
                "lookback": lookback,
                "ma": ma,
                **meta_extra,
            },
        }

        out_json = SIGNALS_DIR / f"{signal_id}.json"
        out_csv = SIGNALS_DIR / f"{signal_id}.csv"
        out_json.write_text(json.dumps(signal_obj, indent=2, ensure_ascii=False), encoding="utf-8")
        _write_signal_csv(out_csv, positions)

        status_path.write_text(json.dumps({"signal_id": signal_id, "status": "succeeded", "finished_at": int(time.time())}, ensure_ascii=False), encoding="utf-8")

    except Exception as e:
        status_path.write_text(
            json.dumps({"signal_id": signal_id, "status": "failed", "finished_at": int(time.time()), "error": repr(e)}, ensure_ascii=False),
            encoding="utf-8",
        )

    finally:
        try:
            _job_sem.release()
        except Exception:
            pass


@app.post("/signals/run")
def signals_run(cfg: Dict[str, Any], background: BackgroundTasks, request: Request):
    require_token(request)
    _rate_limit_run(request)
    _validate_signal_cfg(cfg)

    acquired = _job_sem.acquire(blocking=False)
    if not acquired:
        raise HTTPException(status_code=429, detail="too many concurrent runs")

    signal_id = uuid.uuid4().hex
    try:
        background.add_task(run_signal, signal_id, cfg)
    except Exception:
        try:
            _job_sem.release()
        except Exception:
            pass
        raise

    return {"signal_id": signal_id, "status": "queued"}


@app.get("/signals/{signal_id}")
def signals_get(signal_id: str, request: Request):
    require_token(request)
    p = SIGNALS_DIR / f"{signal_id}.json"
    if not p.exists():
        # maybe still running
        st = SIGNALS_DIR / f"{signal_id}.status.json"
        if st.exists():
            return JSONResponse(read_json(st))
        raise HTTPException(status_code=404, detail="signal not found")
    return JSONResponse(read_json(p))


@app.get("/signals/{signal_id}.csv")
def signals_csv(signal_id: str, request: Request):
    require_token(request)
    p = SIGNALS_DIR / f"{signal_id}.csv"
    if not p.exists():
        raise HTTPException(status_code=404, detail="csv not found")
    return FileResponse(str(p), media_type="text/csv")
