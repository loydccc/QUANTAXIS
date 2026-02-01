# QUANTAXIS Product Goals

## Primary goal
Build a **local / internal** quantitative research + signals product that can reliably generate **high-win-rate** (stable) stock selection signals.

- Primary KPI: **win rate stability** (not only total return)
  - tranche win rate (weekly; 2-week hold overlap)
  - portfolio period win rate
  - relative win rate vs benchmark (optional)

## Product scope (phased)

### Phase A — Manual trading workflow (Signals)
- Generate weekly signals (JSON + CSV) with:
  - strict eligibility filters (tradability)
  - stable scoring (versioned, explainable)
  - tranche overlap (2-week hold) to smooth turnover
  - factor attribution output (why chosen)

### Phase B — Automated execution (later)
- After Phase A is stable:
  - paper trading
  - broker connectors + order state machine
  - production risk controls

## Design principles
- Prefer **constraints + stability** over "factor zoo".
- Every change must be **versioned and attributable**.
- Always report win rate and stability metrics.
