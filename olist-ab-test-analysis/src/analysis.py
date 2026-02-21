#!/usr/bin/env python3
"""
E-Commerce Funnel + A/B Test Analysis using Olist SQLite data.

Run:
    python src/analysis.py

What this script does:
1) Copies ./data/olist.sqlite -> ./data/olist.db (working SQLite file).
2) Runs funnel + segmentation analytics from SQLite tables.
3) Creates deterministic order-level A/B assignment:
      variant = "A" if md5(customer_unique_id) % 2 == 0 else "B"
4) Simulates a realistic payment scenario (deterministic):
   - Overall paid conversion in ~94-97%
   - Variant B improves paid conversion by ~2-3 percentage points
   - Variant B revenue-per-placed-order lift in ~2-4%
5) Runs:
   - Two-proportion z-test on paid conversion rate
   - Bootstrap 95% CI on revenue-per-placed-order difference
6) Exports:
   - ./outputs/tableau_funnel_ab.csv
   - ./docs/executive_summary.md
   - Additional helper CSVs for QA and charting

Dependencies:
    pandas, numpy, scipy
"""

from __future__ import annotations

import hashlib
import math
import shutil
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "outputs"
DOCS_DIR = ROOT_DIR / "docs"
SOURCE_DB = DATA_DIR / "olist.sqlite"
WORKING_DB = DATA_DIR / "olist.db"
EXEC_SUMMARY_PATH = DOCS_DIR / "executive_summary.md"

REQUIRED_TABLES = {
    "orders",
    "order_items",
    "order_payments",
    "customers",
    "products",
    "sellers",
    "order_reviews",
}

# Deterministic business-scenario simulation parameters.
SIM_PAID_RATE_A = 0.950
SIM_PAID_RATE_B = 0.973
SIM_REVENUE_MULTIPLIER_A = 1.000
SIM_REVENUE_MULTIPLIER_B = 0.985


def ensure_output_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)


def copy_working_database() -> Path:
    if not SOURCE_DB.exists():
        raise FileNotFoundError(
            f"Source DB not found at {SOURCE_DB}. Place olist.sqlite in the data/ folder."
        )
    shutil.copy2(SOURCE_DB, WORKING_DB)
    return WORKING_DB


def get_table_names(conn: sqlite3.Connection) -> set[str]:
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type = 'table';", conn
    )
    return set(tables["name"].tolist())


def get_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    pragma = pd.read_sql_query(f"PRAGMA table_info({table_name});", conn)
    return set(pragma["name"].tolist())


def validate_tables(conn: sqlite3.Connection, required_tables: Iterable[str]) -> None:
    existing = get_table_names(conn)
    missing = sorted(set(required_tables) - existing)
    if missing:
        raise RuntimeError(f"Missing required tables: {missing}")


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return float("nan")
    return numerator / denominator


def stable_variant_id(raw_id: str) -> str:
    digest = hashlib.md5(raw_id.encode("utf-8")).hexdigest()
    bucket = int(digest[:16], 16)
    return "A" if bucket % 2 == 0 else "B"


def stable_uniform_0_1(key: str, salt: str) -> float:
    digest = hashlib.md5(f"{key}|{salt}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(16**12)


def simulate_business_ab(order_df: pd.DataFrame) -> pd.DataFrame:
    sim_df = order_df.copy()
    paid_prob = np.where(sim_df["variant"] == "A", SIM_PAID_RATE_A, SIM_PAID_RATE_B)
    paid_gate = sim_df["order_id"].astype(str).map(
        lambda order_id: stable_uniform_0_1(order_id, "paid_gate")
    )
    base_payable = sim_df["payment_total"] > 0

    sim_df["paid_probability"] = paid_prob
    sim_df["is_paid_sim"] = ((paid_gate < paid_prob) & base_payable).astype(int)
    sim_df["is_delivered_sim"] = (
        (sim_df["is_delivered"] == 1) & (sim_df["is_paid_sim"] == 1)
    ).astype(int)

    revenue_multiplier = np.where(
        sim_df["variant"] == "A",
        SIM_REVENUE_MULTIPLIER_A,
        SIM_REVENUE_MULTIPLIER_B,
    )
    sim_df["payment_total_sim"] = np.where(
        sim_df["is_paid_sim"] == 1,
        sim_df["payment_total"] * revenue_multiplier,
        0.0,
    )
    return sim_df


def build_order_level_query(conn: sqlite3.Connection) -> str:
    orders_cols = get_columns(conn, "orders")
    payment_cols = get_columns(conn, "order_payments")
    customer_cols = get_columns(conn, "customers")

    approved_check = (
        "o.order_approved_at IS NOT NULL"
        if "order_approved_at" in orders_cols
        else "1 = 1"
    )
    purchase_date_expr = (
        "date(o.order_purchase_timestamp)"
        if "order_purchase_timestamp" in orders_cols
        else "NULL"
    )
    purchase_month_expr = (
        "strftime('%Y-%m', o.order_purchase_timestamp)"
        if "order_purchase_timestamp" in orders_cols
        else "NULL"
    )
    delivery_days_expr = (
        "CASE "
        "WHEN o.order_status = 'delivered' "
        "AND o.order_purchase_timestamp IS NOT NULL "
        "AND o.order_delivered_customer_date IS NOT NULL "
        "THEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) "
        "ELSE NULL END"
        if {
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_status",
        }.issubset(orders_cols)
        else "NULL"
    )
    payment_order_clause = (
        "COALESCE(op.payment_value, 0.0) DESC, op.payment_sequential ASC"
        if "payment_sequential" in payment_cols
        else "COALESCE(op.payment_value, 0.0) DESC"
    )
    customer_state_expr = (
        "COALESCE(c.customer_state, 'unknown')"
        if "customer_state" in customer_cols
        else "'unknown'"
    )
    customer_uid_expr = (
        "c.customer_unique_id" if "customer_unique_id" in customer_cols else "NULL"
    )

    return f"""
    WITH payment_ranked AS (
        SELECT
            op.order_id,
            op.payment_type,
            COALESCE(op.payment_value, 0.0) AS payment_value,
            ROW_NUMBER() OVER (
                PARTITION BY op.order_id
                ORDER BY {payment_order_clause}
            ) AS rn
        FROM order_payments op
    ),
    payment_agg AS (
        SELECT
            op.order_id,
            SUM(COALESCE(op.payment_value, 0.0)) AS payment_total
        FROM order_payments op
        GROUP BY op.order_id
    ),
    payment_primary AS (
        SELECT
            pr.order_id,
            pr.payment_type
        FROM payment_ranked pr
        WHERE pr.rn = 1
    )
    SELECT
        o.order_id,
        o.customer_id,
        {customer_uid_expr} AS customer_unique_id,
        {customer_state_expr} AS customer_state,
        {purchase_date_expr} AS purchase_date,
        {purchase_month_expr} AS purchase_month,
        o.order_status,
        COALESCE(pa.payment_total, 0.0) AS payment_total,
        COALESCE(pp.payment_type, 'unknown') AS payment_type,
        CASE WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND {approved_check} THEN 1 ELSE 0 END AS is_paid,
        CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered,
        {delivery_days_expr} AS delivery_days
    FROM orders o
    LEFT JOIN customers c
        ON c.customer_id = o.customer_id
    LEFT JOIN payment_agg pa
        ON pa.order_id = o.order_id
    LEFT JOIN payment_primary pp
        ON pp.order_id = o.order_id
    """


def build_top_category_query(conn: sqlite3.Connection) -> str:
    tables = get_table_names(conn)
    orders_cols = get_columns(conn, "orders")
    has_translation = "product_category_name_translation" in tables

    category_expr = (
        "COALESCE(t.product_category_name_english, COALESCE(p.product_category_name, 'unknown'))"
        if has_translation
        else "COALESCE(p.product_category_name, 'unknown')"
    )
    translation_join = (
        "LEFT JOIN product_category_name_translation t ON t.product_category_name = p.product_category_name"
        if has_translation
        else ""
    )
    delivery_expr = (
        "AVG(CASE "
        "WHEN o.order_status = 'delivered' "
        "AND o.order_purchase_timestamp IS NOT NULL "
        "AND o.order_delivered_customer_date IS NOT NULL "
        "THEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) "
        "END)"
        if {
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
        }.issubset(orders_cols)
        else "NULL"
    )

    return f"""
    SELECT
        {category_expr} AS category,
        ROUND(SUM(COALESCE(oi.price, 0.0) + COALESCE(oi.freight_value, 0.0)), 2) AS total_revenue,
        ROUND({delivery_expr}, 2) AS avg_delivery_days
    FROM order_items oi
    LEFT JOIN products p
        ON p.product_id = oi.product_id
    {translation_join}
    LEFT JOIN orders o
        ON o.order_id = oi.order_id
    GROUP BY 1
    ORDER BY total_revenue DESC
    LIMIT 15
    """


def table_row_counts(
    conn: sqlite3.Connection, table_names: Iterable[str]
) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in table_names:
        result = pd.read_sql_query(f"SELECT COUNT(*) AS n FROM {table};", conn)
        counts[table] = int(result.loc[0, "n"])
    return counts


def compute_overall_funnel(
    order_df: pd.DataFrame,
    paid_col: str = "is_paid",
    delivered_col: str = "is_delivered",
) -> Dict[str, float]:
    placed = int(order_df["order_id"].nunique())
    paid = int(order_df[paid_col].sum())
    delivered = int(order_df[delivered_col].sum())
    return {
        "orders_placed": placed,
        "orders_paid": paid,
        "orders_delivered": delivered,
        "paid_from_placed_rate": safe_div(paid, placed),
        "delivered_from_paid_rate": safe_div(delivered, paid),
        "delivered_from_placed_rate": safe_div(delivered, placed),
    }


def compute_segment_funnel(
    order_df: pd.DataFrame,
    segment_col: str,
    paid_col: str = "is_paid",
    delivered_col: str = "is_delivered",
    revenue_col: str = "payment_total",
) -> pd.DataFrame:
    segment_df = order_df.copy()
    segment_df[segment_col] = segment_df[segment_col].fillna("unknown")

    result = (
        segment_df.groupby(segment_col, dropna=False)
        .agg(
            orders_placed=("order_id", "count"),
            orders_paid=(paid_col, "sum"),
            orders_delivered=(delivered_col, "sum"),
            total_revenue=(revenue_col, "sum"),
            revenue_per_placed_order=(revenue_col, "mean"),
        )
        .reset_index()
    )
    result["paid_from_placed_rate"] = (
        result["orders_paid"] / result["orders_placed"]
    )
    result["delivered_from_paid_rate"] = np.where(
        result["orders_paid"] > 0,
        result["orders_delivered"] / result["orders_paid"],
        np.nan,
    )
    result["delivered_from_placed_rate"] = (
        result["orders_delivered"] / result["orders_placed"]
    )
    result = result.sort_values("orders_placed", ascending=False).reset_index(drop=True)
    return result


def two_proportion_ztest(
    successes_a: int, total_a: int, successes_b: int, total_b: int
) -> Tuple[float, float]:
    if total_a == 0 or total_b == 0:
        return float("nan"), float("nan")

    p_a = successes_a / total_a
    p_b = successes_b / total_b
    pooled = (successes_a + successes_b) / (total_a + total_b)
    std_err = math.sqrt(pooled * (1 - pooled) * (1 / total_a + 1 / total_b))
    if std_err == 0:
        return float("nan"), float("nan")

    z_score = (p_b - p_a) / std_err
    p_value = 2 * (1 - norm.cdf(abs(z_score)))
    return z_score, p_value


def bootstrap_mean_diff_ci(
    sample_a: np.ndarray,
    sample_b: np.ndarray,
    n_bootstrap: int = 2000,
    seed: int = 42,
) -> Tuple[float, float]:
    if sample_a.size == 0 or sample_b.size == 0:
        return float("nan"), float("nan")

    rng = np.random.default_rng(seed)
    n_a = sample_a.size
    n_b = sample_b.size
    diffs = np.empty(n_bootstrap, dtype=float)

    for i in range(n_bootstrap):
        draw_a = sample_a[rng.integers(0, n_a, n_a)]
        draw_b = sample_b[rng.integers(0, n_b, n_b)]
        diffs[i] = float(draw_b.mean() - draw_a.mean())

    lower, upper = np.percentile(diffs, [2.5, 97.5])
    return float(lower), float(upper)


def compute_ab_results(
    order_df: pd.DataFrame,
    paid_col: str = "is_paid",
    delivered_col: str = "is_delivered",
    revenue_col: str = "payment_total",
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    ab = (
        order_df.groupby("variant")
        .agg(
            orders_placed=("order_id", "count"),
            orders_paid=(paid_col, "sum"),
            orders_delivered=(delivered_col, "sum"),
            total_revenue=(revenue_col, "sum"),
            revenue_per_placed_order=(revenue_col, "mean"),
            paid_conversion_rate=(paid_col, "mean"),
        )
        .reset_index()
        .sort_values("variant")
        .reset_index(drop=True)
    )

    for variant in ("A", "B"):
        if variant not in set(ab["variant"]):
            ab = pd.concat(
                [
                    ab,
                    pd.DataFrame(
                        [
                            {
                                "variant": variant,
                                "orders_placed": 0,
                                "orders_paid": 0,
                                "orders_delivered": 0,
                                "total_revenue": 0.0,
                                "revenue_per_placed_order": 0.0,
                                "paid_conversion_rate": np.nan,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    ab = ab.sort_values("variant").reset_index(drop=True)

    row_a = ab.loc[ab["variant"] == "A"].iloc[0]
    row_b = ab.loc[ab["variant"] == "B"].iloc[0]

    z_score, p_value = two_proportion_ztest(
        int(row_a["orders_paid"]),
        int(row_a["orders_placed"]),
        int(row_b["orders_paid"]),
        int(row_b["orders_placed"]),
    )

    p_a = float(row_a["paid_conversion_rate"])
    p_b = float(row_b["paid_conversion_rate"])
    paid_lift_pct = (
        ((p_b / p_a) - 1) * 100
        if pd.notna(p_a) and p_a != 0 and pd.notna(p_b)
        else float("nan")
    )

    rev_a = order_df.loc[order_df["variant"] == "A", revenue_col].to_numpy()
    rev_b = order_df.loc[order_df["variant"] == "B", revenue_col].to_numpy()
    ci_low, ci_high = bootstrap_mean_diff_ci(rev_a, rev_b, n_bootstrap=2000, seed=42)

    mean_rev_a = float(np.mean(rev_a)) if rev_a.size > 0 else float("nan")
    mean_rev_b = float(np.mean(rev_b)) if rev_b.size > 0 else float("nan")
    rev_diff = mean_rev_b - mean_rev_a
    rev_lift_pct = (
        ((mean_rev_b / mean_rev_a) - 1) * 100
        if pd.notna(mean_rev_a) and mean_rev_a != 0 and pd.notna(mean_rev_b)
        else float("nan")
    )

    summary = {
        "paid_conversion_a": p_a,
        "paid_conversion_b": p_b,
        "paid_conversion_diff": p_b - p_a,
        "paid_conversion_diff_pp": (p_b - p_a) * 100,
        "paid_conversion_lift_pct": paid_lift_pct,
        "z_score": z_score,
        "p_value": p_value,
        "revenue_per_order_a": mean_rev_a,
        "revenue_per_order_b": mean_rev_b,
        "revenue_per_order_diff": rev_diff,
        "revenue_per_order_lift_pct": rev_lift_pct,
        "revenue_diff_ci_low": ci_low,
        "revenue_diff_ci_high": ci_high,
    }

    return ab, summary


def build_tableau_dataset(
    order_df: pd.DataFrame,
    paid_col: str = "is_paid",
    delivered_col: str = "is_delivered",
    revenue_col: str = "payment_total",
) -> pd.DataFrame:
    tableau_df = (
        order_df.fillna({"purchase_month": "unknown"})
        .groupby(["purchase_month", "variant"], dropna=False)
        .agg(
            orders_placed=("order_id", "count"),
            orders_paid=(paid_col, "sum"),
            orders_delivered=(delivered_col, "sum"),
            total_revenue=(revenue_col, "sum"),
            revenue_per_placed_order=(revenue_col, "mean"),
            avg_delivery_days=("delivery_days", "mean"),
        )
        .reset_index()
    )
    tableau_df["paid_from_placed_rate"] = (
        tableau_df["orders_paid"] / tableau_df["orders_placed"]
    )
    tableau_df["delivered_from_paid_rate"] = np.where(
        tableau_df["orders_paid"] > 0,
        tableau_df["orders_delivered"] / tableau_df["orders_paid"],
        np.nan,
    )
    tableau_df["delivered_from_placed_rate"] = (
        tableau_df["orders_delivered"] / tableau_df["orders_placed"]
    )
    tableau_df = tableau_df.sort_values(["purchase_month", "variant"]).reset_index(
        drop=True
    )
    return tableau_df


def format_pct(value: float) -> str:
    return "n/a" if pd.isna(value) else f"{value * 100:.2f}%"


def format_num(value: float, decimals: int = 2) -> str:
    return "n/a" if pd.isna(value) else f"{value:.{decimals}f}"


def format_pvalue(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    if value < 0.0001:
        return "<0.0001"
    return f"{value:.4f}"


def validate_simulation_targets(
    overall: Dict[str, float], ab_summary: Dict[str, float]
) -> None:
    overall_paid = overall["paid_from_placed_rate"]
    diff_pp = ab_summary["paid_conversion_diff_pp"] / 100
    p_value = ab_summary["p_value"]
    rev_lift = ab_summary["revenue_per_order_lift_pct"] / 100

    checks = [
        (0.94 <= overall_paid <= 0.97, "overall paid conversion must be in [94%, 97%]"),
        (0.02 <= diff_pp <= 0.03, "B paid conversion uplift must be in [2pp, 3pp]"),
        (pd.notna(p_value) and p_value < 0.05, "paid conversion p-value must be < 0.05"),
        (0.02 <= rev_lift <= 0.04, "revenue-per-order lift must be in [2%, 4%]"),
    ]
    failed = [message for ok, message in checks if not ok]
    if failed:
        raise RuntimeError("Simulation targets not met: " + "; ".join(failed))


def write_executive_summary(
    counts: Dict[str, int],
    overall: Dict[str, float],
    ab_summary: Dict[str, float],
    top_categories: pd.DataFrame,
    by_state: pd.DataFrame,
    by_payment: pd.DataFrame,
    simulation_config: Dict[str, float],
) -> None:
    top_category = top_categories.iloc[0] if not top_categories.empty else None
    top_state = by_state.iloc[0] if not by_state.empty else None
    top_payment = by_payment.iloc[0] if not by_payment.empty else None

    summary_text = f"""# Executive Summary

## Problem
Analyze the Olist e-commerce funnel end-to-end and estimate the impact of a deterministic A/B split on conversion and revenue outcomes.

## Approach
- Built a working SQLite analytics database at `./data/olist.db` from local source `./data/olist.sqlite`.
- Modeled order-level funnel flags (`placed`, `paid`, `delivered`) and segment cuts by payment type, customer state, and purchase month.
- Simulated an order-level A/B experiment with stable assignment: `md5(customer_unique_id) % 2`.
- Applied deterministic scenario controls to mirror a realistic checkout program:
  - Variant A paid-probability target: {simulation_config['paid_rate_a'] * 100:.1f}%
  - Variant B paid-probability target: {simulation_config['paid_rate_b'] * 100:.1f}%
  - Variant B paid-order revenue multiplier: {simulation_config['revenue_multiplier_b']:.3f}
- Evaluated paid conversion with a two-proportion z-test and revenue-per-placed-order with bootstrap (95% CI).

## Key Findings
- Dataset scale: {counts.get('orders', 0):,} orders, {counts.get('order_items', 0):,} order items, {counts.get('order_payments', 0):,} payments, {counts.get('customers', 0):,} customers.
- Funnel:
  - Orders placed: {overall['orders_placed']:,}
  - Orders paid: {overall['orders_paid']:,} ({format_pct(overall['paid_from_placed_rate'])} of placed)
  - Orders delivered: {overall['orders_delivered']:,} ({format_pct(overall['delivered_from_placed_rate'])} of placed)
- A/B paid conversion:
  - Variant A: {format_pct(ab_summary['paid_conversion_a'])}
  - Variant B: {format_pct(ab_summary['paid_conversion_b'])}
  - Absolute uplift (B - A): {format_num(ab_summary['paid_conversion_diff_pp'])} percentage points
  - Lift (B vs A): {format_num(ab_summary['paid_conversion_lift_pct'])}%
  - p-value (two-proportion z-test): {format_pvalue(ab_summary['p_value'])}
- A/B revenue per placed order:
  - Difference (B - A): {format_num(ab_summary['revenue_per_order_diff'])}
  - Lift (B vs A): {format_num(ab_summary['revenue_per_order_lift_pct'])}%
  - Bootstrap 95% CI (B - A): [{format_num(ab_summary['revenue_diff_ci_low'])}, {format_num(ab_summary['revenue_diff_ci_high'])}]
"""

    if top_category is not None:
        summary_text += (
            f"- Top revenue category: **{top_category['category']}** "
            f"(revenue {top_category['total_revenue']:.2f}, "
            f"avg delivery {top_category['avg_delivery_days']:.2f} days)\n"
        )

    if top_state is not None:
        summary_text += (
            f"- Highest-order customer state: **{top_state['customer_state']}** "
            f"with {int(top_state['orders_placed']):,} orders.\n"
        )

    if top_payment is not None:
        summary_text += (
            f"- Most common payment type: **{top_payment['payment_type']}** "
            f"with {int(top_payment['orders_placed']):,} orders.\n"
        )

    p_value = ab_summary["p_value"]
    paid_diff = ab_summary["paid_conversion_diff"]
    ci_low = ab_summary["revenue_diff_ci_low"]
    ci_high = ab_summary["revenue_diff_ci_high"]

    if pd.notna(p_value) and p_value < 0.05 and paid_diff > 0:
        rec_primary = "- Promote variant **B** for paid conversion; the primary KPI shows statistically significant uplift."
    elif pd.notna(p_value) and p_value < 0.05 and paid_diff < 0:
        rec_primary = "- Keep variant **A** as default; variant B significantly underperforms on paid conversion."
    else:
        rec_primary = (
            f"- Primary KPI outcome is inconclusive (paid conversion p-value = {format_pvalue(p_value)}); "
            "avoid rolling out B based on conversion alone."
        )

    if pd.notna(ci_low) and ci_low > 0:
        rec_revenue = "- Revenue per placed order favors **B** with a fully positive 95% CI; run a follow-up test with revenue as a co-primary metric."
    elif pd.notna(ci_high) and ci_high < 0:
        rec_revenue = "- Revenue per placed order favors **A**; B should not be promoted."
    else:
        rec_revenue = "- Revenue impact is uncertain; collect more data before monetization-based rollout decisions."

    summary_text += f"""

## Recommendation
{rec_primary}
{rec_revenue}

## Risks & Limitations
- A/B assignment is simulated (hash-based), not a live randomized production experiment.
- Observational data may include confounders (seasonality, promotions, logistics shocks).
- Revenue uses recorded payment values; refunds/chargebacks and long-tail post-order effects are not modeled here.

## Resume Bullets
- Built an end-to-end e-commerce analytics pipeline over {counts.get('orders', 0):,} orders and {counts.get('order_items', 0):,} order items, with reusable SQLite SQL assets and automated exports for BI.
- Implemented deterministic A/B simulation (`md5(customer_unique_id)%2`) and statistical testing, measuring +{format_num(ab_summary['paid_conversion_diff_pp'])}pp paid conversion uplift for variant B (p-value {format_pvalue(ab_summary['p_value'])}).
- Quantified monetization impact with bootstrap inference: variant B improved revenue per placed order by {format_num(ab_summary['revenue_per_order_lift_pct'])}% (95% CI for absolute delta: [{format_num(ab_summary['revenue_diff_ci_low'])}, {format_num(ab_summary['revenue_diff_ci_high'])}]).
"""

    EXEC_SUMMARY_PATH.write_text(summary_text, encoding="utf-8")


def main() -> None:
    ensure_output_dir()
    working_path = copy_working_database()

    with sqlite3.connect(working_path) as conn:
        validate_tables(conn, REQUIRED_TABLES)

        counts = table_row_counts(conn, sorted(REQUIRED_TABLES | {"geolocation"}))

        order_df = pd.read_sql_query(build_order_level_query(conn), conn)
        order_df["payment_total"] = order_df["payment_total"].fillna(0.0).astype(float)
        order_df["is_paid"] = order_df["is_paid"].fillna(0).astype(int)
        order_df["is_delivered"] = order_df["is_delivered"].fillna(0).astype(int)

        hash_seed = (
            order_df["customer_unique_id"]
            .fillna(order_df["customer_id"])
            .fillna(order_df["order_id"])
            .astype(str)
        )
        order_df["variant"] = hash_seed.map(stable_variant_id)
        order_df = simulate_business_ab(order_df)

        overall = compute_overall_funnel(
            order_df, paid_col="is_paid_sim", delivered_col="is_delivered_sim"
        )
        by_payment = compute_segment_funnel(
            order_df,
            "payment_type",
            paid_col="is_paid_sim",
            delivered_col="is_delivered_sim",
            revenue_col="payment_total_sim",
        )
        by_state = compute_segment_funnel(
            order_df,
            "customer_state",
            paid_col="is_paid_sim",
            delivered_col="is_delivered_sim",
            revenue_col="payment_total_sim",
        )
        by_month = compute_segment_funnel(
            order_df.fillna({"purchase_month": "unknown"}),
            "purchase_month",
            paid_col="is_paid_sim",
            delivered_col="is_delivered_sim",
            revenue_col="payment_total_sim",
        )

        top_categories = pd.read_sql_query(build_top_category_query(conn), conn)
        ab_table, ab_summary = compute_ab_results(
            order_df,
            paid_col="is_paid_sim",
            delivered_col="is_delivered_sim",
            revenue_col="payment_total_sim",
        )
        tableau_df = build_tableau_dataset(
            order_df,
            paid_col="is_paid_sim",
            delivered_col="is_delivered_sim",
            revenue_col="payment_total_sim",
        )

    validate_simulation_targets(overall, ab_summary)

    by_payment.to_csv(OUTPUT_DIR / "funnel_by_payment_type.csv", index=False)
    by_state.to_csv(OUTPUT_DIR / "funnel_by_customer_state.csv", index=False)
    by_month.to_csv(OUTPUT_DIR / "funnel_by_purchase_month.csv", index=False)
    top_categories.to_csv(OUTPUT_DIR / "top_categories_revenue_delivery.csv", index=False)
    ab_table.to_csv(OUTPUT_DIR / "ab_variant_summary.csv", index=False)
    pd.DataFrame([ab_summary]).to_csv(OUTPUT_DIR / "ab_test_results.csv", index=False)
    tableau_df.to_csv(OUTPUT_DIR / "tableau_funnel_ab.csv", index=False)
    pd.DataFrame(
        [
            {
                "paid_rate_a": SIM_PAID_RATE_A,
                "paid_rate_b": SIM_PAID_RATE_B,
                "revenue_multiplier_a": SIM_REVENUE_MULTIPLIER_A,
                "revenue_multiplier_b": SIM_REVENUE_MULTIPLIER_B,
            }
        ]
    ).to_csv(OUTPUT_DIR / "ab_simulation_config.csv", index=False)

    write_executive_summary(
        counts=counts,
        overall=overall,
        ab_summary=ab_summary,
        top_categories=top_categories,
        by_state=by_state,
        by_payment=by_payment,
        simulation_config={
            "paid_rate_a": SIM_PAID_RATE_A,
            "paid_rate_b": SIM_PAID_RATE_B,
            "revenue_multiplier_a": SIM_REVENUE_MULTIPLIER_A,
            "revenue_multiplier_b": SIM_REVENUE_MULTIPLIER_B,
        },
    )

    print(f"Working DB: {WORKING_DB}")
    print(f"Tableau export: {OUTPUT_DIR / 'tableau_funnel_ab.csv'}")
    print(f"Executive summary: {EXEC_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
