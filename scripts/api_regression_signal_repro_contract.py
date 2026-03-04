#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Regression checks for signal reproducibility contract."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import api.signals_impl as si


def _ok(name: str, detail: dict | None = None) -> dict:
    return {"name": name, "ok": True, "detail": detail or {}}


def _fail(name: str, detail: dict | None = None) -> dict:
    return {"name": name, "ok": False, "detail": detail or {}}


def _base_cfg() -> dict:
    return {
        "strategy": "hybrid_baseline_weekly_topk",
        "theme": "a_ex_kcb_bse",
        "rebalance": "weekly",
        "top_k": 10,
        "hold_weeks": 2,
        "tranche_overlap": True,
        "end": "2026-02-12",
        "sealed_date": "2026-02-12",
        "data_version_id": "qa_cn_stock_daily@2026-02-12",
        "manifest_sha256": "a" * 64,
    }


def main():
    checks: list[dict] = []

    # 1) validation requires data_version_id
    c1 = _base_cfg()
    c1.pop("data_version_id", None)
    try:
        si.validate_signal_cfg(c1)
        checks.append(_fail("signals_validate_requires_data_version_id", {"error": "expected HTTPException(400), got success"}))
    except HTTPException as e:
        if int(e.status_code) == 400 and "data_version_id" in str(e.detail):
            checks.append(_ok("signals_validate_requires_data_version_id", {"detail": e.detail}))
        else:
            checks.append(_fail("signals_validate_requires_data_version_id", {"status_code": int(e.status_code), "detail": e.detail}))

    # 2) validation requires manifest_sha256
    c2 = _base_cfg()
    c2["manifest_sha256"] = "bad"
    try:
        si.validate_signal_cfg(c2)
        checks.append(_fail("signals_validate_requires_manifest_sha256", {"error": "expected HTTPException(400), got success"}))
    except HTTPException as e:
        if int(e.status_code) == 400 and "manifest_sha256" in str(e.detail):
            checks.append(_ok("signals_validate_requires_manifest_sha256", {"detail": e.detail}))
        else:
            checks.append(_fail("signals_validate_requires_manifest_sha256", {"status_code": int(e.status_code), "detail": e.detail}))

    # 3) valid config should pass validation
    c3 = _base_cfg()
    try:
        si.validate_signal_cfg(c3)
        checks.append(_ok("signals_validate_accepts_repro_fields"))
    except HTTPException as e:
        checks.append(_fail("signals_validate_accepts_repro_fields", {"status_code": int(e.status_code), "detail": e.detail}))

    # 4) run_signal output must carry same repro fields in meta
    with tempfile.TemporaryDirectory(prefix="qa_signal_repro_") as td:
        outdir = Path(td)
        outdir.mkdir(parents=True, exist_ok=True)

        tranche = {
            "rebalance_date": "2026-02-12",
            "effective_date": "2026-02-13",
            "weights": {
                "000001": 0.10,
                "000002": 0.10,
                "000003": 0.10,
                "000004": 0.10,
                "000005": 0.10,
                "000006": 0.10,
                "000007": 0.10,
                "000008": 0.10,
                "000009": 0.10,
                "000010": 0.10,
            },
        }
        tranche2 = {
            "rebalance_date": "2026-02-05",
            "effective_date": "2026-02-06",
            "weights": dict(tranche["weights"]),
        }

        def fake_run_bt(workdir: Path, cfg: dict, strategy: str):
            return (
                "2026-02-12",
                list(tranche["weights"].keys()),
                {"universe_fingerprint": "ufp_regression", "universe_size": 5000},
                dict(cfg),
            )

        cfg = _base_cfg()
        cfg.update(
            {
                "start": "2019-01-01",
                "health_date": "2026-02-12",
                "ma_mode": "filter",
                "score_mode": "baseline",
            }
        )
        signal_id = "regression_signal_repro_contract"

        with (
            patch.object(si, "SIGNALS_DIR", outdir),
            patch.object(si, "_run_baseline_backtest_to_workdir_with_fallback", fake_run_bt),
            patch.object(si, "_extract_last_tranches_from_positions_csv", lambda *_args, **_kwargs: [dict(tranche), dict(tranche2)]),
            patch.object(si, "_load_health_score", lambda _d: (0.8, "/tmp/mock_hi.json")),
        ):
            si.run_signal(signal_id, cfg)
            p = outdir / f"{signal_id}.json"
            if not p.exists():
                checks.append(_fail("run_signal_meta_contains_repro_fields", {"error": "signal json not generated"}))
            else:
                obj = json.loads(p.read_text(encoding="utf-8"))
                meta = obj.get("meta", {}) or {}
                dvid = meta.get("data_version_id")
                msha = meta.get("manifest_sha256")
                if dvid == cfg["data_version_id"] and msha == cfg["manifest_sha256"]:
                    checks.append(
                        _ok(
                            "run_signal_meta_contains_repro_fields",
                            {"data_version_id": dvid, "manifest_sha256_prefix": str(msha)[:8]},
                        )
                    )
                else:
                    checks.append(
                        _fail(
                            "run_signal_meta_contains_repro_fields",
                            {"data_version_id": dvid, "manifest_sha256": msha},
                        )
                    )

                # 5) regression guard: non-cash scores should not be all zero
                positions = obj.get("positions", []) or []
                non_cash = [p for p in positions if str(p.get("code", "")).upper() != "CASH"]
                non_cash_scores = []
                for p0 in non_cash:
                    try:
                        non_cash_scores.append(float(p0.get("score", 0.0) or 0.0))
                    except Exception:
                        non_cash_scores.append(0.0)
                has_nonzero_score = any(abs(s) > 1e-12 for s in non_cash_scores)
                if has_nonzero_score:
                    checks.append(
                        _ok(
                            "run_signal_scores_not_all_zero",
                            {
                                "non_cash_n": len(non_cash_scores),
                                "score_min": min(non_cash_scores) if non_cash_scores else None,
                                "score_max": max(non_cash_scores) if non_cash_scores else None,
                            },
                        )
                    )
                else:
                    checks.append(
                        _fail(
                            "run_signal_scores_not_all_zero",
                            {
                                "non_cash_n": len(non_cash_scores),
                                "score_min": min(non_cash_scores) if non_cash_scores else None,
                                "score_max": max(non_cash_scores) if non_cash_scores else None,
                            },
                        )
                    )

                # 6) regression guard: positions count should be sane (>= MIN_POS)
                min_pos = int(getattr(si, "MIN_POS", 6))
                pos_n = len(positions)
                if pos_n >= min_pos:
                    checks.append(_ok("run_signal_positions_n_ge_min_pos", {"positions_n": pos_n, "min_pos": min_pos}))
                else:
                    checks.append(_fail("run_signal_positions_n_ge_min_pos", {"positions_n": pos_n, "min_pos": min_pos}))

    ok = all(bool(c.get("ok")) for c in checks)
    out = {"ok": ok, "n_checks": len(checks), "checks": checks}
    print(json.dumps(out, ensure_ascii=False))
    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":
    main()
