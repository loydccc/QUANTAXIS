# Alerting Spec (HI v1)

This is the *production* alert contract. Each alert must define:
- threshold
- evaluation frequency
- action / owner

## Alert #1: health_score < 0.2 for >= 3 consecutive trading days
- Threshold: health_score < 0.2 (from daily cache)
- Frequency: once per day after HI cache generation
- Trigger: streak >= 3
- Action: page operator; consider switching to reduced trading / review market regime

## Alert #2: pct_exposure_at_floor rolling 20d > 60%
- Metric: fraction of days in last 20 trading days where exposure==0.4
- Frequency: once per day
- Trigger: > 0.60
- Action: page operator; investigate if HI is overly pessimistic or data issue

## Alert #3: health_missing == true
- Trigger: run_signal meta.health.health_missing == true
- Frequency: on every signal generation
- Action: page operator; fix HI cache pipeline

## Alert #4: health_score > 0.6 but portfolio continues to weaken
- Definition (minimal): if health_score > 0.6 for 5 trading days AND equity drawdown over same window < -X
- Frequency: daily
- Action: page operator; investigate alpha degradation vs HI mismatch

Owners: fill in runbook.
