#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Baseline backtests (mid/low-frequency) for the CN theme seed universe.

Supports strategies:
1) xsec_momentum_weekly_topk: cross-sectional momentum, weekly rebalance, long-only top K equal-weight.
2) xsec_momentum_weekly_invvol: momentum top K, weighted by inverse trailing vol (risk-aware), with max weight cap.
3) ts_ma_weekly: time-series MA filter per-asset, weekly rebalance, long-only.

Data source:
- Prefer **versioned local snapshot**: bars.parquet + manifest.json (reproducible)
- Fallback (legacy): MongoDB collection stock_day
- Fields: code, date (YYYY-MM-DD), close, vol/volume/amount (for liquidity filter)

Outputs (written to outdir):
- metrics.json
- equity.csv
- positions.csv

Notes:
- Uses close-to-close returns.
- Rebalance on last available trading day of each ISO week.
- Signals use only information up to rebalance date.
- Positions are applied starting next trading day (T+1) to avoid look-ahead.
- Simple linear cost model: cost_bps * turnover (one-way) applied on rebalance-effective days.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
import pymongo


def _sha256_file(p: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256_json(obj: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(obj)).hexdigest()


def _load_snapshot_bars(snapshot_dir: str, data_version_id: str, manifest_sha256: str) -> pd.DataFrame:
    """Load bars from a versioned snapshot directory and verify manifest/file hashes."""
    sdir = Path(snapshot_dir)
    mp = sdir / "manifest.json"
    bp = sdir / "bars.parquet"
    if not mp.exists() or not bp.exists():
        raise RuntimeError(f"snapshot dir missing manifest.json or bars.parquet: {sdir}")

    manifest = json.loads(mp.read_text(encoding="utf-8"))
    # verify manifest sha
    calc_msha = _sha256_json({k: v for k, v in manifest.items() if k != "manifest_sha256"})
    if calc_msha != manifest.get("manifest_sha256"):
        raise RuntimeError("manifest self-hash mismatch")
    if manifest_sha256 and manifest.get("manifest_sha256") != manifest_sha256:
        raise RuntimeError("manifest_sha256 does not match request")
    if data_version_id and manifest.get("data_version_id") != data_version_id:
        raise RuntimeError("data_version_id does not match request")

    # verify file hash
    want_files = manifest.get("files") or []
    want_hash = None
    for f in want_files:
        if (f.get("path") or "").endswith("bars.parquet"):
            want_hash = f.get("sha256")
            break
    if want_hash:
        got_hash = _sha256_file(bp)
        if got_hash != want_hash:
            raise RuntimeError("bars.parquet sha256 mismatch")

    df = pd.read_parquet(bp)
    return df


def _panel_from_snapshot(bars: pd.DataFrame, codes: List[str], start: str, end: str) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], Optional[str]]:
    if bars.empty:
        raise RuntimeError("empty bars snapshot")

    # normalize
    if "vol" in bars.columns and "volume" not in bars.columns:
        bars["volume"] = bars["vol"]

    bars = bars.copy()
    bars["date"] = pd.to_datetime(bars["date"], errors="coerce")
    bars = bars.dropna(subset=["date", "code", "close"])
    bars["code"] = bars["code"].astype(str).str.zfill(6)

    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    bars = bars[(bars["date"] >= start_dt) & (bars["date"] <= end_dt) & (bars["code"].isin(codes))]
    if bars.empty:
        raise RuntimeError("no data found in snapshot for selected universe")

    close_panel = bars.pivot(index="date", columns="code", values="close").sort_index()

    # choose a liquidity field if possible
    vol_panel = None
    volume_field = None
    if "volume" in bars.columns:
        volume_field = "volume"
        vol_panel = bars.pivot(index="date", columns="code", values="volume").sort_index()
    elif "amount" in bars.columns:
        volume_field = "amount"
        vol_panel = bars.pivot(index="date", columns="code", values="amount").sort_index()

    return close_panel, vol_panel, volume_field


@dataclass
class MongoCfg:
    host: str
    port: int
    db: str
    user: str
    password: str
    root_user: str
    root_password: str


def get_mongo_cfg() -> MongoCfg:
    return MongoCfg(
        host=os.getenv("MONGODB_HOST", "mongodb"),
        port=int(os.getenv("MONGODB_PORT", "27017")),
        db=os.getenv("MONGODB_DATABASE", "quantaxis"),
        user=os.getenv("MONGODB_USER", "quantaxis"),
        password=os.getenv("MONGODB_PASSWORD", "quantaxis"),
        root_user=os.getenv("MONGO_ROOT_USER", "root"),
        root_password=os.getenv("MONGO_ROOT_PASSWORD", "root"),
    )


def mongo_client(cfg: MongoCfg) -> pymongo.MongoClient:
    uris = [
        f"mongodb://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
        f"mongodb://{cfg.root_user}:{cfg.root_password}@{cfg.host}:{cfg.port}/{cfg.db}?authSource=admin",
    ]
    last = None
    for uri in uris:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=8000)
            c.admin.command("ping")
            return c
        except Exception as e:
            last = e
    raise last  # type: ignore[misc]


def load_universe(theme: str) -> List[str]:
    """Return the base universe by theme.

    - For curated themes (default): load from watchlists/themes_seed_cn.json.
    - Special themes:
      - hs10: 沪深主板 10%（排除创业板/科创板/北交所/新三板等）
      - cyb20: 创业板 20%（300/301）
      - a_ex_kcb_bse: 沪深主板 + 创业板（仅排除科创板 688 与北交所/新三板等）

    Note: Special themes are derived from Mongo collections (stock_list preferred).
    """

    theme = (theme or "all").strip()

    def _is_hs10(code: str) -> bool:
        # SH main board: 600/601/603/605
        # SZ main board: 000/001/002/003
        # Exclude ChiNext 300/301, STAR 688, and others (NQ/BSE etc.)
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith(("300", "301", "688")):
            return False
        if code.startswith(("8", "4")):
            return False
        return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003"))

    def _is_cyb20(code: str) -> bool:
        if not code or len(code) != 6 or not code.isdigit():
            return False
        # ChiNext (创业板): 300/301
        return code.startswith(("300", "301"))

    def _is_a_ex_kcb_bse(code: str) -> bool:
        # Include: main boards + ChiNext
        # Exclude: STAR (688) and BSE/NQ (8/4)
        if not code or len(code) != 6 or not code.isdigit():
            return False
        if code.startswith("688"):
            return False
        if code.startswith(("8", "4")):
            return False
        return code.startswith(("600", "601", "603", "605", "000", "001", "002", "003", "300", "301"))

    if theme in {"hs10", "cn_hs10", "a_hs10"} or theme in {"cyb20", "cn_cyb20", "a_cyb20"} or theme in {"a_ex_kcb_bse", "cn_a_ex_kcb_bse", "a_no_kcb_bse"}:
        cfg = get_mongo_cfg()
        client = mongo_client(cfg)
        db = client[cfg.db]

        codes: set[str] = set()
        coll = db.get_collection("stock_list")
        try:
            n = coll.estimated_document_count()
        except Exception:
            n = 0

        if n and n > 0:
            # stock_list schema varies; try code/ts_code
            for doc in coll.find({}, {"_id": 0, "code": 1, "ts_code": 1}):
                c = doc.get("code")
                if not c and doc.get("ts_code"):
                    c = str(doc.get("ts_code")).split(".")[0]
                if c:
                    codes.add(str(c).zfill(6))
        else:
            # fallback: derive from stock_day
            for c in db["stock_day"].distinct("code"):
                if c:
                    codes.add(str(c).zfill(6))

        if theme in {"hs10", "cn_hs10", "a_hs10"}:
            out = sorted([c for c in codes if _is_hs10(c)])
        elif theme in {"cyb20", "cn_cyb20", "a_cyb20"}:
            out = sorted([c for c in codes if _is_cyb20(c)])
        else:
            out = sorted([c for c in codes if _is_a_ex_kcb_bse(c)])

        if not out:
            raise RuntimeError(f"empty universe for theme={theme} (check stock_list/stock_day)")
        return out

    # Default: curated seeds
    obj = json.loads(Path("watchlists/themes_seed_cn.json").read_text(encoding="utf-8"))
    codes = set()
    for t in obj["themes"]:
        if theme == "all" or t["theme"] == theme:
            for c in t["seed_codes"]:
                codes.add(str(c).zfill(6))
    return sorted(codes)


def detect_volume_field(coll: pymongo.collection.Collection) -> Optional[str]:
    """Best-effort detection of a liquidity field in Mongo docs."""
    sample = coll.find_one({}, {"_id": 0, "vol": 1, "volume": 1, "amount": 1, "money": 1})
    if not sample:
        return None
    for k in ["volume", "vol", "amount", "money"]:
        if k in sample and sample.get(k) is not None:
            return k
    return None


def fetch_panel(
    coll: pymongo.collection.Collection,
    codes: List[str],
    start: str,
    end: str,
    volume_field: Optional[str],
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Fetch close (and optional volume/amount) panels."""
    close_series = {}
    vol_series = {} if volume_field else None

    proj = {"_id": 0, "date": 1, "close": 1}
    if volume_field:
        proj[volume_field] = 1

    # Dates are normalized to ISO strings (YYYY-MM-DD) by migration.
    for code in codes:
        cursor = coll.find(
            {
                "code": code,
                "date": {"$gte": start, "$lte": end},
            },
            proj,
        ).sort("date", 1)
        rows = list(cursor)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.drop_duplicates(subset=["date"]).set_index("date")

        if "close" in df.columns:
            close = df["close"].astype(float)
            close_series[code] = close

        if volume_field and volume_field in df.columns and vol_series is not None:
            vol = pd.to_numeric(df[volume_field], errors="coerce")
            vol_series[code] = vol

    if not close_series:
        raise RuntimeError("no data found for selected universe")

    close_panel = pd.concat(close_series, axis=1).sort_index()
    vol_panel = None
    if vol_series is not None and len(vol_series) > 0:
        vol_panel = pd.concat(vol_series, axis=1).sort_index()

    return close_panel, vol_panel


def pick_weekly_rebalance_dates(index: pd.DatetimeIndex, max_incomplete_gap_days: int = 2) -> List[pd.Timestamp]:
    """Pick weekly rebalance dates as the last trading day of each (W-FRI) week.

    We prefer **complete weeks** for production signals. If the final week bucket ends on Friday
    but our data stops earlier in the week (e.g. end is Tuesday), that bucket is considered
    *incomplete* and will be dropped.

    Heuristic for holidays: if the last available trading day is close to the bucket end
    (<= max_incomplete_gap_days), we keep it (e.g. Fri is a holiday so Thu is the last trading day).
    """
    idx = pd.to_datetime(index)
    if len(idx) == 0:
        return []

    # Ensure monotonic, unique timestamps
    idx = pd.DatetimeIndex(idx).sort_values().unique()

    # Group trading days into weeks that *end on Friday*.
    s = pd.Series(idx, index=idx)
    last = s.groupby(pd.Grouper(freq="W-FRI")).max().dropna()
    if last.empty:
        return []

    # Drop the last bucket if it looks like a partial week (end mid-week), unless it's plausibly
    # a holiday-shortened week.
    bucket_end = pd.Timestamp(last.index[-1]).normalize()
    last_day = pd.Timestamp(last.iloc[-1]).normalize()
    gap_days = int((bucket_end - last_day).days)
    if gap_days > int(max_incomplete_gap_days):
        last = last.iloc[:-1]

    return list(pd.to_datetime(last.values))


def compute_weights_xsec_mom(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    lookback: int,
    top_k: int,
) -> pd.DataFrame:
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)

    for d in rebalance_dates:
        if d not in close.index:
            continue
        loc = close.index.get_loc(d)
        if isinstance(loc, slice):
            loc = loc.stop - 1
        if loc < lookback:
            continue

        window = close.iloc[loc - lookback : loc + 1]
        mom = window.iloc[-1] / window.iloc[0] - 1.0
        mom = mom.dropna()
        if mom.empty:
            continue
        winners = mom.sort_values(ascending=False).head(top_k).index
        w = pd.Series(0.0, index=close.columns)
        w.loc[winners] = 1.0 / len(winners)
        weights.loc[d] = w

    return weights


def _cap_and_normalize(w: pd.Series, max_weight: float) -> pd.Series:
    w = w.clip(lower=0.0)
    if w.sum() <= 0:
        return w * 0.0
    w = w / w.sum()
    if max_weight is None or max_weight <= 0 or max_weight >= 1:
        return w

    w = w.copy()
    for _ in range(10):
        over = w > max_weight
        if not over.any():
            break
        excess = (w[over] - max_weight).sum()
        w[over] = max_weight
        under = w < max_weight
        if under.sum() == 0:
            break
        w[under] = w[under] + excess * (w[under] / w[under].sum())
    if w.sum() > 0:
        w = w / w.sum()
    return w


def compute_weights_xsec_mom_invvol(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    lookback: int,
    top_k: int,
    vol_window: int,
    max_weight: float,
) -> pd.DataFrame:
    # Pick winners by momentum; weight by inverse vol.
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    ret = close.pct_change(fill_method=None)
    vol = ret.rolling(vol_window).std()

    for d in rebalance_dates:
        if d not in close.index:
            continue
        loc = close.index.get_loc(d)
        if isinstance(loc, slice):
            loc = loc.stop - 1
        if loc < max(lookback, vol_window):
            continue

        window = close.iloc[loc - lookback : loc + 1]
        mom = window.iloc[-1] / window.iloc[0] - 1.0
        mom = mom.dropna()
        if mom.empty:
            continue
        winners = mom.sort_values(ascending=False).head(top_k).index

        inv = (1.0 / (vol.loc[d, winners].replace(0.0, np.nan))).replace([np.inf, -np.inf], np.nan).dropna()
        if inv.empty:
            w = pd.Series(0.0, index=close.columns)
        else:
            w_sub = _cap_and_normalize(inv, max_weight=max_weight)
            w = pd.Series(0.0, index=close.columns)
            w.loc[w_sub.index] = w_sub.values
        weights.loc[d] = w

    return weights


def compute_weights_ts_ma(
    close: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    ma_window: int,
) -> pd.DataFrame:
    weights = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)

    ma = close.rolling(ma_window).mean()

    for d in rebalance_dates:
        if d not in close.index:
            continue
        # signal at date d, effective next day
        sig = (close.loc[d] > ma.loc[d]).astype(float)
        sig = sig.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if sig.sum() <= 0:
            w = pd.Series(0.0, index=close.columns)
        else:
            w = sig / sig.sum()
        weights.loc[d] = w

    return weights


def backtest_close_to_close(
    close: pd.DataFrame,
    weights_on_rebalance: pd.DataFrame,
    cost_bps: float,
) -> Tuple[pd.Series, pd.DataFrame, pd.Series, pd.Series]:
    w = weights_on_rebalance.reindex(close.index).ffill().fillna(0.0)
    w_eff = w.shift(1).fillna(0.0)

    daily_ret = close.pct_change(fill_method=None).fillna(0.0)
    gross = (w_eff * daily_ret).sum(axis=1)

    turnover = w_eff.diff().abs().sum(axis=1) / 2.0
    cost = (cost_bps / 10000.0) * turnover
    net = gross - cost

    equity = (1.0 + net).cumprod()
    return equity, w_eff, turnover, net


def build_trades_from_positions(
    close: pd.DataFrame,
    positions: pd.DataFrame,
    eps: float = 1e-12,
) -> pd.DataFrame:
    """Derive a minimal trade blotter from daily target weights.

    Assumptions:
    - We treat changes in target weights as rebalance trades.
    - We record weights (not shares). Execution model is outside baseline.

    Output schema (minimal, stable):
    - date, code, weight_before, weight_after, delta_weight, side, price_close
    """
    if positions.empty:
        return pd.DataFrame(columns=[
            "date",
            "code",
            "weight_before",
            "weight_after",
            "delta_weight",
            "side",
            "price_close",
        ])

    pos = positions.fillna(0.0)
    prev = pos.shift(1).fillna(0.0)
    delta = pos - prev

    rows = []
    for dt in pos.index:
        d = delta.loc[dt]
        changed = d.abs() > eps
        if not bool(changed.any()):
            continue
        for code in d.index[changed]:
            w0 = float(prev.at[dt, code])
            w1 = float(pos.at[dt, code])
            dw = float(d.at[code])
            side = "buy" if dw > 0 else "sell"
            px = None
            try:
                if dt in close.index and code in close.columns:
                    v = close.at[dt, code]
                    px = None if pd.isna(v) else float(v)
            except Exception:
                px = None
            rows.append(
                {
                    "date": dt,
                    "code": code,
                    "weight_before": w0,
                    "weight_after": w1,
                    "delta_weight": dw,
                    "side": side,
                    "price_close": px,
                }
            )

    return pd.DataFrame(rows)


def perf_stats(equity: pd.Series, net_ret: pd.Series, turnover: pd.Series) -> Dict:
    n = len(net_ret)
    ann = 252
    cagr = float(equity.iloc[-1] ** (ann / max(n, 1)) - 1.0) if n > 1 else 0.0
    vol = float(net_ret.std() * np.sqrt(ann))
    sharpe = float((net_ret.mean() * ann) / (net_ret.std() * np.sqrt(ann) + 1e-12))
    peak = equity.cummax()
    dd = equity / peak - 1.0
    max_dd = float(dd.min())

    avg_turnover = float(turnover.mean())
    turnover_annual = float(turnover.sum() / (n / ann)) if n > 0 else 0.0

    return {
        "bars": int(n),
        "cagr": cagr,
        "vol": vol,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "final_equity": float(equity.iloc[-1]),
        "avg_daily_turnover": avg_turnover,
        "annual_turnover": turnover_annual,
    }


def norm_date(s: str) -> str:
    s = s.strip()
    if "-" in s:
        return s
    if len(s) == 8:
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    raise ValueError(f"bad date: {s}")


def universe_fingerprint(codes: List[str], cfg: Dict) -> str:
    payload = {
        "codes": codes,
        "cfg": cfg,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def audit_from_close_panel(close: pd.DataFrame) -> Dict:
    # basic coverage diagnostics
    bars_per_code = close.notna().sum(axis=0).astype(int)
    return {
        "universe_size_raw": int(close.shape[1]),
        "bars_min": int(bars_per_code.min()) if len(bars_per_code) else 0,
        "bars_max": int(bars_per_code.max()) if len(bars_per_code) else 0,
        "panel_missing_ratio": float(close.isna().sum().sum() / max(close.size, 1)),
    }


def write_outputs(
    outdir: Path,
    equity: pd.Series,
    positions: pd.DataFrame,
    trades: pd.DataFrame,
    stats: Dict,
    turnover: pd.Series,
    net_ret: pd.Series,
) -> None:
    """Write artifacts.

    Policy:
    - Keep legacy CSV names for compatibility.
    - Also write parquet-first standardized artifacts for downstream tooling.
    """
    outdir.mkdir(parents=True, exist_ok=True)

    # Standard run metadata (lightweight): use the same payload as metrics for now.
    # This keeps us on the "artifact spec" track without inventing a new framework.
    (outdir / "run.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    (outdir / "metrics.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")

    equity_df = pd.DataFrame({"date": equity.index, "equity": equity.values})
    # Legacy
    pd.DataFrame({"date": equity.index.strftime("%Y-%m-%d"), "equity": equity.values}).to_csv(
        outdir / "equity.csv", index=False
    )
    # Standard
    equity_df.to_parquet(outdir / "equity_curve.parquet", index=False)

    # Standard: daily series needed for analysis and later trading loop alignment
    pd.DataFrame({"date": net_ret.index, "net_ret": net_ret.values}).to_parquet(
        outdir / "returns.parquet", index=False
    )
    pd.DataFrame({"date": turnover.index, "turnover": turnover.values}).to_parquet(
        outdir / "turnover.parquet", index=False
    )

    pos = positions.copy()
    pos.insert(0, "date", pos.index)
    # Legacy
    pos_legacy = pos.copy()
    pos_legacy["date"] = pos_legacy["date"].dt.strftime("%Y-%m-%d")
    pos_legacy.to_csv(outdir / "positions.csv", index=False)
    # Standard
    pos.to_parquet(outdir / "positions.parquet", index=False)

    # Trades (standard + optional legacy)
    if trades is None or trades.empty:
        # still write empty parquet for schema stability
        pd.DataFrame(
            columns=[
                "date",
                "code",
                "weight_before",
                "weight_after",
                "delta_weight",
                "side",
                "price_close",
            ]
        ).to_parquet(outdir / "trades.parquet", index=False)
    else:
        t = trades.copy()
        # standard
        t.to_parquet(outdir / "trades.parquet", index=False)
        # legacy (best-effort)
        t_legacy = t.copy()
        if "date" in t_legacy.columns:
            t_legacy["date"] = pd.to_datetime(t_legacy["date"]).dt.strftime("%Y-%m-%d")
        t_legacy.to_csv(outdir / "trades.csv", index=False)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--theme", default="all")
    ap.add_argument(
        "--strategy",
        default="xsec_momentum_weekly_topk",
        choices=["xsec_momentum_weekly_topk", "xsec_momentum_weekly_invvol", "ts_ma_weekly"],
    )
    ap.add_argument("--lookback", type=int, default=60)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--ma", type=int, default=60)
    ap.add_argument("--vol-window", type=int, default=20, help="trailing trading days for vol weighting")
    ap.add_argument("--max-weight", type=float, default=0.10, help="max single-asset weight cap")
    ap.add_argument("--cost-bps", type=float, default=10.0)
    ap.add_argument("--min-bars", type=int, default=252, help="min non-NaN close bars required per code")
    ap.add_argument("--liq-window", type=int, default=0, help="recent trading days window for close+volume eligibility (0 disables)")
    ap.add_argument("--liq-min-ratio", type=float, default=1.0, help="min fraction of days in window that must have valid close and volume>0")
    ap.add_argument("--outdir", default="/tmp/output")

    # reproducible snapshot inputs (preferred)
    ap.add_argument("--snapshot-dir", default=None, help="dir containing bars.parquet + manifest.json")
    ap.add_argument("--data-version-id", default=os.getenv("QUANTAXIS_DATA_VERSION_ID", ""), help="e.g. qa_cn_stock_daily@2026-02-01")
    ap.add_argument("--manifest-sha256", default=os.getenv("QUANTAXIS_MANIFEST_SHA256", ""), help="64-hex manifest sha")
    ap.add_argument("--require-snapshot", default=os.getenv("QUANTAXIS_REQUIRE_SNAPSHOT", "0"), help="1 to forbid Mongo fallback")

    args = ap.parse_args(argv)

    start = norm_date(args.start)
    end = norm_date(args.end)

    codes = load_universe(args.theme)

    require_snapshot = str(args.require_snapshot).strip() in {"1", "true", "yes"}

    # Prefer snapshot (reproducible)
    close_raw: pd.DataFrame
    vol_raw: Optional[pd.DataFrame]
    volume_field: Optional[str]

    if args.snapshot_dir:
        bars = _load_snapshot_bars(
            snapshot_dir=str(args.snapshot_dir),
            data_version_id=str(args.data_version_id or ""),
            manifest_sha256=str(args.manifest_sha256 or ""),
        )
        close_raw, vol_raw, volume_field = _panel_from_snapshot(bars, codes, start, end)
    else:
        if require_snapshot:
            raise RuntimeError("snapshot required but --snapshot-dir not provided")
        # Legacy fallback: MongoDB
        cfg = get_mongo_cfg()
        client = mongo_client(cfg)
        db = client[cfg.db]
        coll = db["stock_day"]

        volume_field = detect_volume_field(coll)
        close_raw, vol_raw = fetch_panel(coll, codes, start, end, volume_field=volume_field)
        close_raw = close_raw.sort_index()
        if vol_raw is not None:
            vol_raw = vol_raw.sort_index()

    # Universe eligibility (tradability / can-run strategy):
    # IMPORTANT semantic shift:
    # - args.min_bars is a *factor availability* threshold (handled upstream in gating/attribution),
    #   NOT a global universe eligibility filter.
    # - Here we only require enough price history to run the chosen baseline strategy.
    bars_per_code = close_raw.notna().sum(axis=0).astype(int)

    # Minimal history to compute the strategy signals at all.
    # Add a small buffer for weekly rebalance alignment + stability.
    required_bars = max(5, int(args.lookback), int(args.ma), int(args.vol_window)) + 5

    eligible = bars_per_code[bars_per_code >= int(required_bars)].index.tolist()
    dropped = sorted(set(close_raw.columns) - set(eligible))

    # Liquidity/suspension filter (optional): require recent close+volume to be present.
    if int(args.liq_window) > 0:
        if volume_field is None or vol_raw is None:
            raise RuntimeError("no volume field found in Mongo (expected one of: volume/vol/amount/money)")
        win = int(args.liq_window)
        ratio = float(args.liq_min_ratio)
        ratio = max(0.0, min(1.0, ratio))

        # take last win dates available in panel
        last_idx = close_raw.index[-win:]
        close_recent = close_raw.loc[last_idx, eligible]
        vol_recent = vol_raw.reindex(index=last_idx).loc[:, eligible]

        close_ok = (close_recent.notna().sum(axis=0) >= int(win * ratio))
        vol_ok = ((vol_recent.fillna(0.0) > 0).sum(axis=0) >= int(win * ratio))
        eligible2 = [c for c in eligible if close_ok.get(c, False) and vol_ok.get(c, False)]
        dropped += sorted(set(eligible) - set(eligible2))
        eligible = eligible2

    close = close_raw[eligible]

    if close.shape[1] == 0:
        raise RuntimeError(f"no eligible codes after filters (min_bars={args.min_bars}, liq_window={args.liq_window})")

    reb_dates = pick_weekly_rebalance_dates(close.index)

    if args.strategy == "xsec_momentum_weekly_topk":
        weights = compute_weights_xsec_mom(close, reb_dates, lookback=args.lookback, top_k=args.top)
    elif args.strategy == "xsec_momentum_weekly_invvol":
        weights = compute_weights_xsec_mom_invvol(
            close,
            reb_dates,
            lookback=args.lookback,
            top_k=args.top,
            vol_window=args.vol_window,
            max_weight=args.max_weight,
        )
    else:
        weights = compute_weights_ts_ma(close, reb_dates, ma_window=args.ma)

    equity, positions, turnover, net_ret = backtest_close_to_close(close, weights, cost_bps=args.cost_bps)
    trades = build_trades_from_positions(close=close, positions=positions)

    stats = perf_stats(equity, net_ret, turnover)
    run_cfg = {
        "strategy": args.strategy,
        "theme": args.theme,
        "start": start,
        "end": end,
        "min_bars": int(args.min_bars),
        "liq_window": int(args.liq_window),
        "liq_min_ratio": float(args.liq_min_ratio),
        "cost_bps": float(args.cost_bps),
        "params": {
            "lookback": int(args.lookback),
            "top": int(args.top),
            "ma": int(args.ma),
            "vol_window": int(args.vol_window),
            "max_weight": float(args.max_weight),
        },
        "data": {
            "source": "snapshot" if args.snapshot_dir else "mongo",
            "data_version_id": str(args.data_version_id or "") if args.snapshot_dir else None,
            "manifest_sha256": str(args.manifest_sha256 or "") if args.snapshot_dir else None,
            "collection": "stock_day" if not args.snapshot_dir else None,
            "price": "close",
            "liquidity": volume_field,
            "adjustment": "none",
        },
    }

    stats.update(
        {
            **run_cfg,
            "universe_size": int(close.shape[1]),
            "universe_size_raw": int(close_raw.shape[1]),
            "universe_dropped": dropped,
            "universe_fingerprint": universe_fingerprint(sorted(close.columns.tolist()), run_cfg),
            "data_audit": audit_from_close_panel(close),
            "start_effective": str(close.index.min().date()),
            "end_effective": str(close.index.max().date()),
            "generated_at": int(time.time()),
        }
    )

    outdir = Path(args.outdir)
    write_outputs(outdir, equity, positions, trades, stats, turnover=turnover, net_ret=net_ret)
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
