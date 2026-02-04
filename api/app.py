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

# Routers (thin wrappers)
from api.signals import router as signals_router


from api.core import REPORTS_DIR, RUNS_DIR, SIGNALS_DIR, read_json, redact_text
from api.security import require_token, rate_limit_run, validate_cfg_envelope

ROOT = Path(__file__).resolve().parents[1]

app = FastAPI(title="QUANTAXIS API", version="0.1.0")
app.include_router(signals_router)

# --- Security / hardening knobs (env) ---
API_MAX_CONCURRENT = int(os.getenv("QUANTAXIS_API_MAX_CONCURRENT", "2"))
API_JOB_TIMEOUT_SEC = int(os.getenv("QUANTAXIS_API_JOB_TIMEOUT_SEC", "3600"))
API_LOG_TAIL = int(os.getenv("QUANTAXIS_API_LOG_TAIL", "2000"))
API_INCLUDE_LOGS = os.getenv("QUANTAXIS_API_INCLUDE_LOGS", "").strip().lower() in {"1", "true", "yes"}

# --- Factor score config (versioned in signal meta) ---
FAC_WINDOWS = {
    "ret_10d": int(os.getenv("QUANTAXIS_FAC_RET_10D", "10")),
    "ret_20d": int(os.getenv("QUANTAXIS_FAC_RET_20D", "20")),
    "vol_20d": int(os.getenv("QUANTAXIS_FAC_VOL_20D", "20")),
    "liq_20d": int(os.getenv("QUANTAXIS_FAC_LIQ_20D", "20")),
}
FAC_WEIGHTS = {
    "ret_20d": float(os.getenv("QUANTAXIS_SCORE_W_RET_20D", "1.0")),
    "ret_10d": float(os.getenv("QUANTAXIS_SCORE_W_RET_10D", "0.5")),
    "vol_20d": float(os.getenv("QUANTAXIS_SCORE_W_VOL_20D", "-0.5")),
    "liq_20d": float(os.getenv("QUANTAXIS_SCORE_W_LIQ_20D", "0.2")),
}

# --- Hard threshold filters (tradability/risk) ---
# Set to >0 to enable by default; request cfg may override.
HARD_VOL_20D_MAX = float(os.getenv("QUANTAXIS_HARD_VOL_20D_MAX", "0"))
HARD_LIQ_20D_MIN = float(os.getenv("QUANTAXIS_HARD_LIQ_20D_MIN", "0"))

# In-memory concurrency (good enough for local/one-process MVP)
_job_sem = threading.BoundedSemaphore(max(1, API_MAX_CONCURRENT))


_strategy_re = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


def _validate_cfg(cfg: Dict[str, Any]) -> None:
    """Config sanity checks (MVP hardening).

    Not a full schema yet, but blocks common abuse:
    - over-large payloads
    - very deep nesting
    - pathological strings/keys
    - unexpected types for key fields

    Additionally, we require data version fingerprints for reproducible backtests.
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

    # --- reproducibility requirements (data version fingerprints) ---
    # We keep these fields at top-level for now to avoid premature schema frameworks.
    if not isinstance(cfg.get("data_version_id"), str) or not cfg.get("data_version_id"):
        raise HTTPException(status_code=400, detail="missing data_version_id")
    if not isinstance(cfg.get("manifest_sha256"), str) or len(cfg.get("manifest_sha256")) != 64:
        raise HTTPException(status_code=400, detail="missing or bad manifest_sha256")

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


# NOTE: moved to api/core.py (kept as import: read_json, redact_text)

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
    <li><code>GET /signals/{signal_id}_factors.csv</code></li>
  </ul>
  <p>Tip: if you opened <code>""" + base + """</code> in a browser, 404 on <code>/</code> is now fixed.</p>
</body>
</html>
"""
    )


@app.get("/health")
def health():
    # auth_required is derived from api.security.API_TOKEN
    from api.security import API_TOKEN, API_RUNS_PER_MIN

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


def _allowed_report_files(run_id: str) -> Optional[set[str]]:
    """Return allowlist of files for a report directory.

    If artifact_manifest.json exists, we only allow files listed in it.
    Otherwise, we fall back to a small legacy allowlist.
    """
    report_dir = REPORTS_DIR / run_id
    mp = report_dir / "artifact_manifest.json"
    if mp.exists():
        try:
            obj = read_json(mp)
            files = obj.get("files") or []
            names = {str(f.get("name")) for f in files if f.get("name")}
            return names
        except Exception:
            # if manifest is corrupt, safest is to deny
            return set()

    return {
        "metrics.json",
        "run.json",
        "equity.csv",
        "positions.csv",
        "equity_curve.parquet",
        "positions.parquet",
        "console.txt",
        "summary.md",
        "manifest.json",
        "artifact_manifest.json",
    }


@app.get("/reports/{run_id}/artifacts")
def report_artifacts(run_id: str):
    p = REPORTS_DIR / run_id / "artifact_manifest.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="artifact_manifest.json not found")
    return JSONResponse(read_json(p))


@app.get("/reports/{run_id}/file/{name}")
def report_file(run_id: str, name: str):
    # allowlist simple filenames to reduce path traversal risk
    if "/" in name or ".." in name:
        raise HTTPException(status_code=400, detail="bad filename")

    allowed = _allowed_report_files(run_id)
    if allowed is not None and name not in allowed:
        raise HTTPException(status_code=404, detail="file not found")

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

    # size/depth guards (signals cfg is not a backtest /run cfg; do NOT require data_version_id/manifest_sha256 here)
    try:
        raw = json.dumps(cfg, ensure_ascii=False)
    except Exception:
        raise HTTPException(status_code=400, detail="config must be JSON-serializable")
    if API_CFG_MAX_BYTES > 0 and len(raw.encode("utf-8")) > API_CFG_MAX_BYTES:
        raise HTTPException(status_code=400, detail="config too large")
    if API_CFG_MAX_DEPTH > 0 and _walk_depth(cfg) > API_CFG_MAX_DEPTH:
        raise HTTPException(status_code=400, detail="config too deeply nested")
    if len(cfg) > 300:
        raise HTTPException(status_code=400, detail="config has too many keys")
    for forbidden in ("cmd", "command", "shell", "cwd", "workdir", "path"):
        if forbidden in cfg:
            raise HTTPException(status_code=400, detail=f"forbidden field: {forbidden}")

    strategy = cfg.get("strategy")
    theme = cfg.get("theme", "all")
    top_k = cfg.get("top_k", 10)
    rebalance = cfg.get("rebalance", "weekly")
    hold_weeks = cfg.get("hold_weeks", 2)
    tranche_overlap = cfg.get("tranche_overlap", True)
    liq_window = cfg.get("liq_window", 20)
    liq_min_ratio = cfg.get("liq_min_ratio", 1.0)
    ma_mode = cfg.get("ma_mode", "filter")
    score_mode = cfg.get("score_mode", "baseline")

    # Execution realism (optional)
    execution_mode = str(cfg.get("execution_mode", "naive"))  # naive|realistic
    backup_k = int(cfg.get("backup_k", 150))
    limit_tiering = bool(cfg.get("limit_tiering", True))
    limit_pct = float(cfg.get("limit_pct", 0.10))
    limit_price_eps_bps = float(cfg.get("limit_price_eps_bps", 5.0))
    limit_touch_mode = str(cfg.get("limit_touch_mode", "hl"))  # hl|close
    limit_touch_eps = float(cfg.get("limit_touch_eps", 1e-6))

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

    if score_mode not in {"baseline", "factor"}:
        raise HTTPException(status_code=400, detail="bad score_mode (baseline|factor)")

    if execution_mode not in {"naive", "realistic"}:
        raise HTTPException(status_code=400, detail="bad execution_mode (naive|realistic)")
    if not isinstance(backup_k, int) or backup_k < 0 or backup_k > 500:
        raise HTTPException(status_code=400, detail="bad backup_k")
    if not (0.0 < limit_pct < 0.5):
        raise HTTPException(status_code=400, detail="bad limit_pct")
    if not (0.0 <= limit_price_eps_bps <= 1000.0):
        raise HTTPException(status_code=400, detail="bad limit_price_eps_bps")
    if limit_touch_mode not in {"hl", "close"}:
        raise HTTPException(status_code=400, detail="bad limit_touch_mode (hl|close)")
    if not (0.0 <= limit_touch_eps <= 0.5):
        raise HTTPException(status_code=400, detail="bad limit_touch_eps")


def _write_signal_csv(path: Path, positions: list[dict]) -> None:
    # minimal CSV (code, weight, rank, score)
    lines = ["code,weight,rank,score"]
    for p in positions:
        lines.append(f"{p['code']},{p['weight']},{p['rank']},{p['score']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_factors_csv(path: Path, positions: list[dict]) -> None:
    cols = [
        "code",
        "weight",
        "rank",
        "score",
        "fac_ret_10d",
        "fac_ret_20d",
        "fac_vol_20d",
        "fac_liq_20d",
        "z_ret_10d",
        "z_ret_20d",
        "z_vol_20d",
        "z_liq_20d",
    ]
    lines = [",".join(cols)]
    for p in positions:
        row = [str(p.get(c, "")) for c in cols]
        lines.append(",".join(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _positions_to_portfolio(positions: list[dict]) -> Dict[str, float]:
    return {p["code"]: float(p["weight"]) for p in positions}


def _portfolio_to_positions(
    port: Dict[str, float],
    scores: Optional[Dict[str, float]] = None,
    factors: Optional[Dict[str, Dict[str, float]]] = None,
    zfactors: Optional[Dict[str, Dict[str, float]]] = None,
) -> list[dict]:
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
        row = {"code": c, "weight": round(w, 10), "rank": i, "score": round((scores or {}).get(c, 0.0), 6)}
        if factors and c in factors:
            row.update({f"fac_{k}": factors[c].get(k) for k in ["ret_10d", "ret_20d", "vol_20d", "liq_20d"]})
        if zfactors and c in zfactors:
            row.update({f"z_{k}": round(zfactors[c].get(k, 0.0), 6) for k in ["ret_10d", "ret_20d", "vol_20d", "liq_20d"]})
        out.append(row)
    return out


def _zscore(s: Dict[str, float]) -> Dict[str, float]:
    vals = [v for v in s.values() if v is not None]
    if not vals:
        return {k: 0.0 for k in s.keys()}
    import math

    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / max(1, (len(vals) - 1))
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd <= 0:
        return {k: 0.0 for k in s.keys()}
    return {k: (v - m) / sd for k, v in s.items()}


def _compute_factors_for_codes(as_of_date: str, codes: list[str], cfg: Dict[str, Any]) -> tuple[Dict[str, Dict[str, float]], Optional[str]]:
    """Compute a small factor pack for codes at as_of_date from Mongo stock_day.

    Returns: {code: {ret_10d, ret_20d, vol_20d, liq_20d}}

    Uses env-configured windows by default; request cfg may override with:
    - fac_ret_10d / fac_ret_20d / fac_vol_20d / fac_liq_20d
    """
    # lazy imports so API can still import without deps in some environments
    import datetime

    try:
        import pymongo  # type: ignore
        import pandas as pd  # type: ignore
    except Exception as e:
        raise RuntimeError(f"missing deps for factor computation: {e!r}")

    # Mongo cfg (same env names as baseline, plus docker .env fallbacks)
    host = os.getenv("MONGODB_HOST", os.getenv("MONGO_HOST", "mongodb"))
    port = int(os.getenv("MONGODB_PORT", os.getenv("MONGO_PORT", "27017")))
    dbn = os.getenv("MONGODB_DATABASE", os.getenv("MONGO_DATABASE", "quantaxis"))

    user = os.getenv("MONGODB_USER", os.getenv("MONGO_USER", "quantaxis"))
    password = os.getenv("MONGODB_PASSWORD", os.getenv("MONGO_PASSWORD", "quantaxis"))

    root_user = os.getenv("MONGO_ROOT_USER", "root")
    root_password = os.getenv("MONGO_ROOT_PASSWORD", "root")

    # Try app user, root user, and finally unauthenticated (some local dev installs).
    uris = [
        f"mongodb://{user}:{password}@{host}:{port}/{dbn}?authSource=admin",
        f"mongodb://{root_user}:{root_password}@{host}:{port}/{dbn}?authSource=admin",
        f"mongodb://{host}:{port}/{dbn}",
    ]
    last = None
    client = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            client = c
            break
        except Exception as e:
            last = e
    if client is None:
        raise RuntimeError(f"mongo connect failed: {last!r}")

    coll = client[dbn]["stock_day"]

    # detect liquidity field
    sample = coll.find_one({}, {"_id": 0, "volume": 1, "vol": 1, "amount": 1, "money": 1})
    liq_field = None
    if sample:
        for k in ["amount", "volume", "vol", "money"]:
            if k in sample and sample.get(k) is not None:
                liq_field = k
                break

    # record detected field for meta
    _DETECTED_LIQ_FIELD = liq_field

    # windows (cfg override > env default)
    ret10 = int(cfg.get("fac_ret_10d", FAC_WINDOWS["ret_10d"]))
    ret20 = int(cfg.get("fac_ret_20d", FAC_WINDOWS["ret_20d"]))
    volw = int(cfg.get("fac_vol_20d", FAC_WINDOWS["vol_20d"]))
    liqw = int(cfg.get("fac_liq_20d", FAC_WINDOWS["liq_20d"]))

    end_dt = pd.to_datetime(as_of_date)
    # pull a bit more than needed to cover weekends/holidays gaps
    start_dt = end_dt - pd.Timedelta(days=120)

    proj = {"_id": 0, "date": 1, "close": 1}
    if liq_field:
        proj[liq_field] = 1

    fac: Dict[str, Dict[str, float]] = {}
    # Support mixed Mongo date formats seen in this repo:
    # - "YYYY-MM-DD" strings
    # - YYYYMMDD ints/strings
    start_s = str(start_dt.date())
    end_s = str(end_dt.date())
    start_i = int(start_dt.strftime("%Y%m%d"))
    end_i = int(end_dt.strftime("%Y%m%d"))

    for code in codes:
        q = {
            "code": code,
            "$or": [
                {"date": {"$gte": start_s, "$lte": end_s}},
                {"date": {"$gte": start_i, "$lte": end_i}},
            ],
        }
        cursor = coll.find(q, proj).sort("date", 1)
        rows = list(cursor)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        # to_datetime can't reliably parse int + str mixtures unless coerced
        df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed")
        df = df.drop_duplicates(subset=["date"]).set_index("date").sort_index()
        if "close" not in df.columns:
            continue
        close = pd.to_numeric(df["close"], errors="coerce")
        close = close.dropna()
        if close.empty or close.index.max() < end_dt:
            # if missing exact as_of_date, use last available <= end_dt
            close = close.loc[close.index <= end_dt]
        if close.shape[0] < max(ret20 + 1, volw + 1):
            continue

        c_end = float(close.iloc[-1])
        r10 = float(c_end / float(close.iloc[-1 - ret10]) - 1.0) if close.shape[0] > ret10 else 0.0
        r20 = float(c_end / float(close.iloc[-1 - ret20]) - 1.0) if close.shape[0] > ret20 else 0.0
        ret = close.pct_change().dropna()
        v20 = float(ret.tail(volw).std()) if ret.shape[0] >= volw else float(ret.std())

        liq = 0.0
        if liq_field and liq_field in df.columns:
            series = pd.to_numeric(df[liq_field], errors="coerce").fillna(0.0)
            series = series.loc[series.index <= end_dt]
            liq = float(series.tail(liqw).mean()) if series.shape[0] >= 1 else 0.0

        fac[code] = {"ret_10d": r10, "ret_20d": r20, "vol_20d": v20, "liq_20d": liq}

    return fac, liq_field


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


def _run_baseline_backtest_to_workdir_with_fallback(
    workdir: Path,
    cfg: Dict[str, Any],
    strategy: str,
) -> tuple[str, list[str], Dict[str, Any], Dict[str, Any]]:
    """Run baseline backtest with a small, deterministic fallback ladder.

    Goal: avoid hard-failing the product workflow when the strict default filters
    (min_bars=800, liq_window=20, liq_min_ratio=1.0) eliminate the entire universe
    for smaller/partial datasets.

    Returns: (as_of_date, picks, stats, cfg_used)
    """

    def _is_empty_universe_err(msg: str) -> bool:
        return "no eligible codes after filters" in (msg or "")

    attempts: list[Dict[str, Any]] = []

    # Copy once; we will mutate per-attempt.
    base = dict(cfg)

    # Fallback ladder (in order):
    # 1) disable liquidity window filter
    # 2) reduce min_bars to 250
    # 3) reduce min_bars to 120
    ladder: list[Dict[str, Any]] = [
        {},
        {"liq_window": 0},
        {"liq_window": 0, "min_bars": min(int(base.get("min_bars", 800)), 250)},
        {"liq_window": 0, "min_bars": min(int(base.get("min_bars", 800)), 120)},
    ]

    last_err: Exception | None = None
    for i, patch in enumerate(ladder, start=1):
        cfg_try = dict(base)
        cfg_try.update(patch)
        try:
            as_of_date, picks, stats = _run_baseline_backtest_to_workdir(workdir, cfg_try, strategy)
            # annotate stats so the signal JSON can reflect what happened
            stats = dict(stats)
            stats["auto_relax"] = attempts
            return as_of_date, picks, stats, cfg_try
        except Exception as e:
            last_err = e
            msg = str(e)
            attempts.append({"attempt": i, "patch": patch, "error": (msg[-400:] if msg else repr(e))})
            # only relax for the empty-universe error; otherwise bubble up.
            if not _is_empty_universe_err(msg):
                raise
            continue

    if last_err:
        raise last_err
    raise RuntimeError("run failed")


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
    score_mode = str(cfg.get("score_mode", "baseline"))
    hold_weeks = int(cfg.get("hold_weeks", 2))
    tranche_overlap = bool(cfg.get("tranche_overlap", True))

    # Execution realism (optional; validated in _validate_signal_cfg)
    execution_mode = str(cfg.get("execution_mode", "naive"))
    backup_k = int(cfg.get("backup_k", 150))
    limit_tiering = bool(cfg.get("limit_tiering", True))
    limit_pct = float(cfg.get("limit_pct", 0.10))
    limit_price_eps_bps = float(cfg.get("limit_price_eps_bps", 5.0))
    limit_touch_mode = str(cfg.get("limit_touch_mode", "hl"))
    limit_touch_eps = float(cfg.get("limit_touch_eps", 1e-6))

    # cfg used for the successful run (may be auto-relaxed)
    cfg_used: Dict[str, Any] = dict(cfg)

    # hard thresholds (cfg override > env default)
    hard_vol_20d_max = float(cfg.get("hard_vol_20d_max", HARD_VOL_20D_MAX))
    hard_liq_20d_min = float(cfg.get("hard_liq_20d_min", HARD_LIQ_20D_MIN))

    try:
        if strategy == "hybrid_baseline_weekly_topk":
            # 1) momentum topK
            mom_dir = workdir / "mom"
            mom_dir.mkdir(parents=True, exist_ok=True)
            mom_date, mom_picks, mom_stats, cfg_used = _run_baseline_backtest_to_workdir_with_fallback(
                mom_dir, cfg, "xsec_momentum_weekly_topk"
            )

            # 2) MA filter (acts like a breadth filter)
            ma_dir = workdir / "ma"
            ma_dir.mkdir(parents=True, exist_ok=True)
            ma_date, ma_picks, ma_stats, cfg_used = _run_baseline_backtest_to_workdir_with_fallback(
                ma_dir, cfg_used, "ts_ma_weekly"
            )

            # tranche overlap extraction
            import pandas as pd

            mom_tr = _extract_last_tranches_from_positions_csv(mom_dir / "positions.csv", n=max(hold_weeks, 1))
            ma_tr = _extract_last_tranches_from_positions_csv(ma_dir / "positions.csv", n=max(hold_weeks, 1))

            # build per-tranche pick lists using latest snapshots (aligned by order)
            tranche_objs = []
            hard_filter_stats = []
            final_port: Dict[str, float] = {}
            final_scores: Dict[str, float] = {}

            n_tr = 1 if not tranche_overlap or hold_weeks <= 1 else min(2, hold_weeks)
            scale = 1.0 / n_tr

            for t in range(n_tr):
                mom_w = mom_tr[t]["weights"] if t < len(mom_tr) else {}
                ma_w = ma_tr[t]["weights"] if t < len(ma_tr) else {}
                # momentum candidates by weight (weights are equal, but keep stable order).
                # IMPORTANT: when we do factor scoring, we need a *larger candidate pool* than top_k,
                # otherwise flipping factor weights only re-orders picks but rarely changes membership.
                mom_sorted = sorted(mom_w.items(), key=lambda x: (-x[1], x[0]))
                cand_k = int(cfg.get("candidate_k", (top_k * 5 if score_mode == "factor" else top_k)))
                cand_k = max(top_k, min(500, cand_k))
                mom_top = [c for c, _w in mom_sorted][:cand_k]
                ma_set = set([c for c, w in ma_w.items() if w > 0])

                scores: Dict[str, float] = {}
                mom_rank: Dict[str, int] = {}
                for i, code in enumerate(mom_top, start=1):
                    mom_rank[code] = i
                    scores[code] = float(top_k - i + 1)
                if ma_mode == "boost":
                    for code in ma_set:
                        scores[code] = scores.get(code, 0.0) + 1.0

                # optional factor score overrides baseline scoring
                factor_pack: Dict[str, Dict[str, float]] = {}
                if score_mode == "factor":
                    factor_pack, _liq_field = _compute_factors_for_codes(mom_tr[t]["rebalance_date"], list(set(mom_top) | ma_set), cfg)
                    # build cross-sectional z-scores
                    r10 = {c: factor_pack[c]["ret_10d"] for c in factor_pack}
                    r20 = {c: factor_pack[c]["ret_20d"] for c in factor_pack}
                    v20 = {c: factor_pack[c]["vol_20d"] for c in factor_pack}
                    liq = {c: factor_pack[c]["liq_20d"] for c in factor_pack}
                    zr10, zr20, zv20, zliq = _zscore(r10), _zscore(r20), _zscore(v20), _zscore(liq)
                    # weights (cfg override > env default)
                    w = {
                        "ret_20d": float(cfg.get("score_w_ret_20d", FAC_WEIGHTS["ret_20d"])),
                        "ret_10d": float(cfg.get("score_w_ret_10d", FAC_WEIGHTS["ret_10d"])),
                        "vol_20d": float(cfg.get("score_w_vol_20d", FAC_WEIGHTS["vol_20d"])),
                        "liq_20d": float(cfg.get("score_w_liq_20d", FAC_WEIGHTS["liq_20d"])),
                    }
                    for c in factor_pack:
                        scores[c] = (
                            w["ret_20d"] * zr20.get(c, 0.0)
                            + w["ret_10d"] * zr10.get(c, 0.0)
                            + w["vol_20d"] * zv20.get(c, 0.0)
                            + w["liq_20d"] * zliq.get(c, 0.0)
                        )

                if ma_mode == "filter":
                    candidates = set(mom_top) & ma_set
                    if len(candidates) < max(3, min(5, top_k)):
                        candidates = set(ma_set)
                else:
                    candidates = set(mom_top) | ma_set

                # hard threshold filtering (only when factor pack is available)
                hard_stats = {"before": len(candidates), "after": len(candidates), "vol_20d_max": hard_vol_20d_max, "liq_20d_min": hard_liq_20d_min}
                if score_mode == "factor" and factor_pack and (hard_vol_20d_max > 0 or hard_liq_20d_min > 0):
                    def _ok(code: str) -> bool:
                        fp = factor_pack.get(code)
                        if not fp:
                            return False
                        if hard_vol_20d_max > 0 and fp.get("vol_20d", 0.0) > hard_vol_20d_max:
                            return False
                        if hard_liq_20d_min > 0 and fp.get("liq_20d", 0.0) < hard_liq_20d_min:
                            return False
                        return True

                    candidates = {c for c in candidates if _ok(c)}
                    hard_stats["after"] = len(candidates)

                hard_filter_stats.append({"tranche": t, **hard_stats})

                def sort_key(code: str):
                    return (-scores.get(code, 0.0), mom_rank.get(code, 10**9), code)

                ranked = sorted(candidates, key=sort_key)
                picks = ranked[:top_k]
                backups = ranked[top_k : top_k + max(0, backup_k)]

                # Execution realism: if enabled, apply limit-touch freeze on BUY side and fill with backups.
                if execution_mode == "realistic" and backups:
                    # Determine which picks are new buys vs previous tranche holdings (mom/ma overlap makes it fuzzy,
                    # but we treat prev tranche holdings as "already held" for buy-block purposes).
                    prev_set = set()
                    if t - 1 >= 0 and t - 1 < len(tranche_objs):
                        prev_set = set(tranche_objs[t - 1].get("picks") or [])

                    # Compute limit-touch on rebalance date using Mongo OHLC.
                    # We reuse factor computation helper to connect to Mongo.
                    try:
                        import pandas as pd
                        asof = pd.to_datetime(mom_tr[t]["rebalance_date"]) if t < len(mom_tr) else pd.to_datetime(mom_date)
                    except Exception:
                        asof = None

                    blocked_buys = set()
                    if asof is not None:
                        # connect mongo (same env as factor compute)
                        import pymongo
                        host = os.getenv("MONGODB_HOST", os.getenv("MONGO_HOST", "mongodb"))
                        port = int(os.getenv("MONGODB_PORT", os.getenv("MONGO_PORT", "27017")))
                        dbn = os.getenv("MONGODB_DATABASE", os.getenv("MONGO_DATABASE", "quantaxis"))
                        user = os.getenv("MONGODB_USER", os.getenv("MONGO_USER", "quantaxis"))
                        password = os.getenv("MONGODB_PASSWORD", os.getenv("MONGO_PASSWORD", "quantaxis"))
                        root_user = os.getenv("MONGO_ROOT_USER", "root")
                        root_password = os.getenv("MONGO_ROOT_PASSWORD", "root")
                        uris = [
                            f"mongodb://{user}:{password}@{host}:{port}/{dbn}?authSource=admin",
                            f"mongodb://{root_user}:{root_password}@{host}:{port}/{dbn}?authSource=admin",
                            f"mongodb://{host}:{port}/{dbn}",
                        ]
                        client = None
                        for uri in uris:
                            try:
                                c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
                                c.admin.command("ping")
                                client = c
                                break
                            except Exception:
                                pass
                        if client is not None:
                            coll = client[dbn]["stock_day"]

                            # helper: limit pct tiering
                            def _lpct(code: str) -> float:
                                if limit_tiering and str(code).startswith(("300", "301")):
                                    return 0.20
                                return float(limit_pct)

                            # helper: near in bps
                            def _near(a: float, b: float) -> bool:
                                if b == 0:
                                    return abs(a - b) <= 1e-6
                                return abs(a - b) <= abs(b) * (float(limit_price_eps_bps) / 10000.0)

                            # candidates that are buys (not in prev_set)
                            buy_list = [c for c in picks if c not in prev_set]
                            if buy_list:
                                # fetch today + prev close
                                end = asof
                                start = asof - pd.Timedelta(days=10)
                                start_s = str(start.date()); end_s = str(end.date())
                                start2 = start.strftime("%Y%m%d"); end2 = end.strftime("%Y%m%d")

                                for code in buy_list:
                                    code6 = str(code).zfill(6)
                                    q = {"code": code6, "$or": [{"date": {"$gte": start_s, "$lte": end_s}}, {"date": {"$gte": start2, "$lte": end2}}]}
                                    rows = list(coll.find(q, {"_id": 0, "date": 1, "high": 1, "low": 1, "close": 1}).sort("date", 1))
                                    if len(rows) < 2:
                                        continue
                                    df = pd.DataFrame(rows)
                                    df["date"] = pd.to_datetime(df["date"].astype(str), format="mixed", errors="coerce")
                                    df = df.dropna(subset=["date"]).sort_values("date")
                                    df = df[df["date"] <= asof]
                                    if df.shape[0] < 2:
                                        continue
                                    today = df.iloc[-1]
                                    prev = df.iloc[-2]
                                    pc = float(prev.get("close", 0.0) or 0.0)
                                    hh = float(today.get("high", 0.0) or 0.0)
                                    cc = float(today.get("close", 0.0) or 0.0)
                                    # touch mode
                                    lp = _lpct(code6)
                                    up_lim = pc * (1.0 + lp)
                                    if limit_touch_mode == "hl":
                                        touch_up = hh >= up_lim and _near(hh, up_lim)
                                    else:
                                        touch_up = abs(cc - hh) <= float(limit_touch_eps) and _near(cc, up_lim)
                                    if touch_up:
                                        blocked_buys.add(code)

                    if blocked_buys:
                        # remove blocked buys and fill from backups
                        keep = [c for c in picks if c not in blocked_buys]
                        held = set(prev_set) | set(keep)
                        add = []
                        for c in backups:
                            if c not in held:
                                add.append(c)
                            if len(add) >= len(blocked_buys):
                                break
                        picks = keep + add

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

            # attach factor attribution for final holdings when factor scoring is enabled
            as_of_date = tranche_objs[0]["rebalance_date"] if tranche_objs else (mom_date or ma_date)

            factors = None
            zfactors = None
            liq_field_detected = None
            if score_mode == "factor" and final_port:
                factors, liq_field_detected = _compute_factors_for_codes(as_of_date, list(final_port.keys()), cfg)
                r10 = {c: factors[c]["ret_10d"] for c in factors}
                r20 = {c: factors[c]["ret_20d"] for c in factors}
                v20 = {c: factors[c]["vol_20d"] for c in factors}
                liq = {c: factors[c]["liq_20d"] for c in factors}
                zr10, zr20, zv20, zliq = _zscore(r10), _zscore(r20), _zscore(v20), _zscore(liq)
                zfactors = {c: {"ret_10d": zr10.get(c, 0.0), "ret_20d": zr20.get(c, 0.0), "vol_20d": zv20.get(c, 0.0), "liq_20d": zliq.get(c, 0.0)} for c in factors}

            positions = _portfolio_to_positions(final_port, scores=final_scores, factors=factors, zfactors=zfactors)

            # meta: keep both components for debugging
            stats = mom_stats
            meta_extra = {
                "ma_mode": ma_mode,
                "score_mode": score_mode,
                "execution_mode": execution_mode,
                "backup_k": backup_k,
                "limit_tiering": limit_tiering,
                "limit_pct": limit_pct,
                "limit_price_eps_bps": limit_price_eps_bps,
                "limit_touch_mode": limit_touch_mode,
                "limit_touch_eps": limit_touch_eps,
                "score_weights": {
                    "ret_20d": float(cfg.get("score_w_ret_20d", FAC_WEIGHTS["ret_20d"])),
                    "ret_10d": float(cfg.get("score_w_ret_10d", FAC_WEIGHTS["ret_10d"])),
                    "vol_20d": float(cfg.get("score_w_vol_20d", FAC_WEIGHTS["vol_20d"])),
                    "liq_20d": float(cfg.get("score_w_liq_20d", FAC_WEIGHTS["liq_20d"])),
                },
                "factor_windows": {
                    "ret_10d": int(cfg.get("fac_ret_10d", FAC_WINDOWS["ret_10d"])),
                    "ret_20d": int(cfg.get("fac_ret_20d", FAC_WINDOWS["ret_20d"])),
                    "vol_20d": int(cfg.get("fac_vol_20d", FAC_WINDOWS["vol_20d"])),
                    "liq_20d": int(cfg.get("fac_liq_20d", FAC_WINDOWS["liq_20d"])),
                },
                "liq_field_detected": liq_field_detected,
                "hard_filters": {"vol_20d_max": hard_vol_20d_max, "liq_20d_min": hard_liq_20d_min},
                "hard_filter_stats": hard_filter_stats,
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
            base_date, base_picks, stats, cfg_used = _run_baseline_backtest_to_workdir_with_fallback(workdir, cfg, strategy)
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

            as_of_date = tranche_objs[0]["rebalance_date"] if tranche_objs else base_date

            factors = None
            zfactors = None
            liq_field_detected = None
            if score_mode == "factor" and final_port:
                factors, liq_field_detected = _compute_factors_for_codes(as_of_date, list(final_port.keys()), cfg)
                r10 = {c: factors[c]["ret_10d"] for c in factors}
                r20 = {c: factors[c]["ret_20d"] for c in factors}
                v20 = {c: factors[c]["vol_20d"] for c in factors}
                liq = {c: factors[c]["liq_20d"] for c in factors}
                zr10, zr20, zv20, zliq = _zscore(r10), _zscore(r20), _zscore(v20), _zscore(liq)
                zfactors = {c: {"ret_10d": zr10.get(c, 0.0), "ret_20d": zr20.get(c, 0.0), "vol_20d": zv20.get(c, 0.0), "liq_20d": zliq.get(c, 0.0)} for c in factors}

            positions = _portfolio_to_positions(final_port, factors=factors, zfactors=zfactors)
            meta_extra = {
                "score_mode": score_mode,
                "score_weights": {
                    "ret_20d": float(cfg.get("score_w_ret_20d", FAC_WEIGHTS["ret_20d"])),
                    "ret_10d": float(cfg.get("score_w_ret_10d", FAC_WEIGHTS["ret_10d"])),
                    "vol_20d": float(cfg.get("score_w_vol_20d", FAC_WEIGHTS["vol_20d"])),
                    "liq_20d": float(cfg.get("score_w_liq_20d", FAC_WEIGHTS["liq_20d"])),
                },
                "factor_windows": {
                    "ret_10d": int(cfg.get("fac_ret_10d", FAC_WINDOWS["ret_10d"])),
                    "ret_20d": int(cfg.get("fac_ret_20d", FAC_WINDOWS["ret_20d"])),
                    "vol_20d": int(cfg.get("fac_vol_20d", FAC_WINDOWS["vol_20d"])),
                    "liq_20d": int(cfg.get("fac_liq_20d", FAC_WINDOWS["liq_20d"])),
                },
                "liq_field_detected": liq_field_detected,
                "hard_filters": {"vol_20d_max": hard_vol_20d_max, "liq_20d_min": hard_liq_20d_min},
                "hold_weeks": hold_weeks,
                "tranche_overlap": tranche_overlap,
                "tranches": tranche_objs,
            }
        # version signature for reproducibility
        # If we had to auto-relax filters, reflect the *effective* values in the signal meta.
        min_bars_eff = int(cfg_used.get("min_bars", min_bars))
        liq_window_eff = int(cfg_used.get("liq_window", liq_window))
        liq_min_ratio_eff = float(cfg_used.get("liq_min_ratio", liq_min_ratio))

        meta_base = {
            "strategy": strategy,
            "theme": theme,
            "top_k": top_k,
            "rebalance": "weekly",
            "min_bars": min_bars_eff,
            "liq_window": liq_window_eff,
            "liq_min_ratio": liq_min_ratio_eff,
            "lookback": lookback,
            "ma": ma,
            # include auto-relax trace (if any) for transparency
            "auto_relax": stats.get("auto_relax") if isinstance(stats, dict) else None,
            **meta_extra,
        }
        meta_sig = json.dumps(meta_base, sort_keys=True, ensure_ascii=False)
        import hashlib

        config_signature = hashlib.sha256(meta_sig.encode("utf-8")).hexdigest()

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
                "config_signature": config_signature,
                "universe_fingerprint": stats.get("universe_fingerprint"),
                "universe_size": stats.get("universe_size"),
                **meta_base,
            },
        }

        out_json = SIGNALS_DIR / f"{signal_id}.json"
        out_csv = SIGNALS_DIR / f"{signal_id}.csv"
        out_fcsv = SIGNALS_DIR / f"{signal_id}_factors.csv"
        out_json.write_text(json.dumps(signal_obj, indent=2, ensure_ascii=False), encoding="utf-8")
        _write_signal_csv(out_csv, positions)
        # optional richer output
        _write_factors_csv(out_fcsv, positions)

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


# Signals routes are provided by api/signals.py router (see app.include_router).
