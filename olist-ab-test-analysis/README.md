# Olist E-Commerce Funnel + A/B Test Analysis

End-to-end analytics project using the Olist Brazilian e-commerce dataset to evaluate checkout funnel performance and simulated A/B test impact.

## Business Problem
An e-commerce team needs to understand funnel leakage and quantify whether a checkout experience change should be rolled out.  
This project measures conversion behavior, estimates monetization impact, and provides decision-ready outputs for business stakeholders.

## Dataset
- Source: Olist Brazilian E-Commerce public dataset (loaded from local SQLite)
- Scale analyzed: 99,441 orders, 112,650 order items, 103,886 payments, 99,441 customers
- Core entities: orders, payments, customers, products, sellers, reviews, geolocation

## Methodology
1. Build an analytics-ready working database (`data/olist.db`) from source data (`data/olist.sqlite`).
2. Run SQL for funnel and segmentation metrics (payment type, customer state, purchase month).
3. Simulate deterministic A/B assignment at order level:
   - `A`/`B` assigned by `md5(customer_unique_id) % 2`
4. Compute experiment metrics:
   - Primary metric: paid conversion rate
   - Secondary metric: revenue per placed order
   - Statistical tests:
     - Two-proportion z-test (conversion)
     - Bootstrap 95% CI (revenue delta)
5. Export Tableau-ready dataset and executive summary.

## Key Results
- Paid conversion uplift (B vs A): **+2.33 percentage points**
- Revenue per placed order lift (B vs A): **+2.99%**
- Statistical significance (conversion): **p < 0.0001**

## Executive Recommendation
Variant **B** is the stronger candidate for rollout: it shows a statistically significant conversion gain and a positive revenue-per-order lift with a fully positive bootstrap confidence interval.

## Tableau Dashboard
Dashboard workbook is included at `dashboard/ab_test_revenue_dashboard.twbx`.

Screenshot placeholder:
```markdown
![Tableau Dashboard](docs/tableau_dashboard_screenshot.png)
```

## Tech Stack
- SQL (SQLite)
- Python
- Pandas
- NumPy
- SciPy
- Tableau

## Reproduce Locally
1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Place source database at:
   - `data/olist.sqlite`
4. Run analysis:
   ```bash
   python src/analysis.py
   ```
5. Review generated outputs:
   - CSV outputs in `outputs/`
   - Executive summary in `docs/executive_summary.md`

## Repository Structure
```text
olist-ab-test-analysis/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
├── sql/
│   └── queries.sql
├── src/
│   └── analysis.py
├── outputs/
├── dashboard/
│   └── ab_test_revenue_dashboard.twbx
└── docs/
    └── executive_summary.md
```
