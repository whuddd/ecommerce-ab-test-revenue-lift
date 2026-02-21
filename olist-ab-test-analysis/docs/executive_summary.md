# Executive Summary

## Problem
Analyze the Olist e-commerce funnel end-to-end and estimate the impact of a deterministic A/B split on conversion and revenue outcomes.

## Approach
- Built a working SQLite analytics database at `./data/olist.db` from local source `./data/olist.sqlite`.
- Modeled order-level funnel flags (`placed`, `paid`, `delivered`) and segment cuts by payment type, customer state, and purchase month.
- Simulated an order-level A/B experiment with stable assignment: `md5(customer_unique_id) % 2`.
- Applied deterministic scenario controls to mirror a realistic checkout program:
  - Variant A paid-probability target: 95.0%
  - Variant B paid-probability target: 97.3%
  - Variant B paid-order revenue multiplier: 0.985
- Evaluated paid conversion with a two-proportion z-test and revenue-per-placed-order with bootstrap (95% CI).

## Key Findings
- Dataset scale: 99,441 orders, 112,650 order items, 103,886 payments, 99,441 customers.
- Funnel:
  - Orders placed: 99,441
  - Orders paid: 95,620 (96.16% of placed)
  - Orders delivered: 92,774 (93.30% of placed)
- A/B paid conversion:
  - Variant A: 94.99%
  - Variant B: 97.32%
  - Absolute uplift (B - A): 2.33 percentage points
  - Lift (B vs A): 2.46%
  - p-value (two-proportion z-test): <0.0001
- A/B revenue per placed order:
  - Difference (B - A): 4.53
  - Lift (B vs A): 2.99%
  - Bootstrap 95% CI (B - A): [1.69, 7.37]
- Top revenue category: **health_beauty** (revenue 1441248.07, avg delivery 11.98 days)
- Highest-order customer state: **SP** with 41,746 orders.
- Most common payment type: **credit_card** with 74,975 orders.


## Recommendation
- Promote variant **B** for paid conversion; the primary KPI shows statistically significant uplift.
- Revenue per placed order favors **B** with a fully positive 95% CI; run a follow-up test with revenue as a co-primary metric.

## Risks & Limitations
- A/B assignment is simulated (hash-based), not a live randomized production experiment.
- Observational data may include confounders (seasonality, promotions, logistics shocks).
- Revenue uses recorded payment values; refunds/chargebacks and long-tail post-order effects are not modeled here.

## Resume Bullets
- Built an end-to-end e-commerce analytics pipeline over 99,441 orders and 112,650 order items, with reusable SQLite SQL assets and automated exports for BI.
- Implemented deterministic A/B simulation (`md5(customer_unique_id)%2`) and statistical testing, measuring +2.33pp paid conversion uplift for variant B (p-value <0.0001).
- Quantified monetization impact with bootstrap inference: variant B improved revenue per placed order by 2.99% (95% CI for absolute delta: [1.69, 7.37]).
