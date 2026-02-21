-- E-Commerce Funnel + Segmentation SQL (SQLite)
-- Dataset: Olist Brazilian E-Commerce
-- Note: These queries assume standard Olist table names:
-- orders, order_payments, customers, order_items, products, product_category_name_translation
-- Note: Deterministic A/B scenario simulation (paid-rate shift + revenue lift controls)
-- is implemented in src/analysis.py after extracting the order-level dataset.

-- ============================================================
-- 0) Base order-level fact CTE (reused by funnel queries)
-- ============================================================
WITH payment_ranked AS (
    SELECT
        op.order_id,
        op.payment_type,
        COALESCE(op.payment_value, 0.0) AS payment_value,
        ROW_NUMBER() OVER (
            PARTITION BY op.order_id
            ORDER BY COALESCE(op.payment_value, 0.0) DESC, op.payment_sequential ASC
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
),
order_level AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.customer_state,
        date(o.order_purchase_timestamp) AS purchase_date,
        strftime('%Y-%m', o.order_purchase_timestamp) AS purchase_month,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_customer_date,
        COALESCE(pa.payment_total, 0.0) AS payment_total,
        COALESCE(pp.payment_type, 'unknown') AS payment_type,
        CASE
            WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND o.order_approved_at IS NOT NULL THEN 1
            ELSE 0
        END AS is_paid,
        CASE
            WHEN o.order_status = 'delivered' THEN 1
            ELSE 0
        END AS is_delivered
    FROM orders o
    LEFT JOIN customers c
        ON c.customer_id = o.customer_id
    LEFT JOIN payment_agg pa
        ON pa.order_id = o.order_id
    LEFT JOIN payment_primary pp
        ON pp.order_id = o.order_id
)
SELECT *
FROM order_level
LIMIT 50;

-- ============================================================
-- 1) Overall funnel metrics
-- ============================================================
WITH payment_agg AS (
    SELECT order_id, SUM(COALESCE(payment_value, 0.0)) AS payment_total
    FROM order_payments
    GROUP BY order_id
),
order_level AS (
    SELECT
        o.order_id,
        CASE
            WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND o.order_approved_at IS NOT NULL THEN 1
            ELSE 0
        END AS is_paid,
        CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered
    FROM orders o
    LEFT JOIN payment_agg pa
        ON pa.order_id = o.order_id
)
SELECT
    COUNT(*) AS orders_placed,
    SUM(is_paid) AS orders_paid,
    SUM(is_delivered) AS orders_delivered,
    ROUND(1.0 * SUM(is_paid) / COUNT(*), 4) AS paid_from_placed_rate,
    ROUND(CASE WHEN SUM(is_paid) = 0 THEN NULL ELSE 1.0 * SUM(is_delivered) / SUM(is_paid) END, 4) AS delivered_from_paid_rate,
    ROUND(1.0 * SUM(is_delivered) / COUNT(*), 4) AS delivered_from_placed_rate
FROM order_level;

-- ============================================================
-- 2) Funnel by payment_type
-- ============================================================
WITH payment_ranked AS (
    SELECT
        op.order_id,
        op.payment_type,
        COALESCE(op.payment_value, 0.0) AS payment_value,
        ROW_NUMBER() OVER (
            PARTITION BY op.order_id
            ORDER BY COALESCE(op.payment_value, 0.0) DESC, op.payment_sequential ASC
        ) AS rn
    FROM order_payments op
),
payment_agg AS (
    SELECT order_id, SUM(COALESCE(payment_value, 0.0)) AS payment_total
    FROM order_payments
    GROUP BY order_id
),
payment_primary AS (
    SELECT order_id, payment_type
    FROM payment_ranked
    WHERE rn = 1
),
order_level AS (
    SELECT
        o.order_id,
        COALESCE(pp.payment_type, 'unknown') AS payment_type,
        CASE
            WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND o.order_approved_at IS NOT NULL THEN 1
            ELSE 0
        END AS is_paid,
        CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered
    FROM orders o
    LEFT JOIN payment_agg pa
        ON pa.order_id = o.order_id
    LEFT JOIN payment_primary pp
        ON pp.order_id = o.order_id
)
SELECT
    payment_type,
    COUNT(*) AS orders_placed,
    SUM(is_paid) AS orders_paid,
    SUM(is_delivered) AS orders_delivered,
    ROUND(1.0 * SUM(is_paid) / COUNT(*), 4) AS paid_from_placed_rate,
    ROUND(CASE WHEN SUM(is_paid) = 0 THEN NULL ELSE 1.0 * SUM(is_delivered) / SUM(is_paid) END, 4) AS delivered_from_paid_rate,
    ROUND(1.0 * SUM(is_delivered) / COUNT(*), 4) AS delivered_from_placed_rate
FROM order_level
GROUP BY payment_type
ORDER BY orders_placed DESC;

-- ============================================================
-- 3) Funnel by customer_state
-- ============================================================
WITH payment_agg AS (
    SELECT order_id, SUM(COALESCE(payment_value, 0.0)) AS payment_total
    FROM order_payments
    GROUP BY order_id
),
order_level AS (
    SELECT
        o.order_id,
        COALESCE(c.customer_state, 'unknown') AS customer_state,
        CASE
            WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND o.order_approved_at IS NOT NULL THEN 1
            ELSE 0
        END AS is_paid,
        CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered
    FROM orders o
    LEFT JOIN customers c
        ON c.customer_id = o.customer_id
    LEFT JOIN payment_agg pa
        ON pa.order_id = o.order_id
)
SELECT
    customer_state,
    COUNT(*) AS orders_placed,
    SUM(is_paid) AS orders_paid,
    SUM(is_delivered) AS orders_delivered,
    ROUND(1.0 * SUM(is_paid) / COUNT(*), 4) AS paid_from_placed_rate,
    ROUND(CASE WHEN SUM(is_paid) = 0 THEN NULL ELSE 1.0 * SUM(is_delivered) / SUM(is_paid) END, 4) AS delivered_from_paid_rate,
    ROUND(1.0 * SUM(is_delivered) / COUNT(*), 4) AS delivered_from_placed_rate
FROM order_level
GROUP BY customer_state
ORDER BY orders_placed DESC;

-- ============================================================
-- 4) Funnel by purchase_month
-- ============================================================
WITH payment_agg AS (
    SELECT order_id, SUM(COALESCE(payment_value, 0.0)) AS payment_total
    FROM order_payments
    GROUP BY order_id
),
order_level AS (
    SELECT
        o.order_id,
        strftime('%Y-%m', o.order_purchase_timestamp) AS purchase_month,
        CASE
            WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND o.order_approved_at IS NOT NULL THEN 1
            ELSE 0
        END AS is_paid,
        CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered
    FROM orders o
    LEFT JOIN payment_agg pa
        ON pa.order_id = o.order_id
)
SELECT
    purchase_month,
    COUNT(*) AS orders_placed,
    SUM(is_paid) AS orders_paid,
    SUM(is_delivered) AS orders_delivered,
    ROUND(1.0 * SUM(is_paid) / COUNT(*), 4) AS paid_from_placed_rate,
    ROUND(CASE WHEN SUM(is_paid) = 0 THEN NULL ELSE 1.0 * SUM(is_delivered) / SUM(is_paid) END, 4) AS delivered_from_paid_rate,
    ROUND(1.0 * SUM(is_delivered) / COUNT(*), 4) AS delivered_from_placed_rate
FROM order_level
GROUP BY purchase_month
ORDER BY purchase_month;

-- ============================================================
-- 5) Top categories by revenue + average delivery time (days)
-- ============================================================
WITH item_level AS (
    SELECT
        oi.order_id,
        COALESCE(p.product_category_name, 'unknown') AS product_category_name,
        COALESCE(t.product_category_name_english, COALESCE(p.product_category_name, 'unknown')) AS product_category_name_english,
        COALESCE(oi.price, 0.0) + COALESCE(oi.freight_value, 0.0) AS item_revenue
    FROM order_items oi
    LEFT JOIN products p
        ON p.product_id = oi.product_id
    LEFT JOIN product_category_name_translation t
        ON t.product_category_name = p.product_category_name
),
order_delivery AS (
    SELECT
        o.order_id,
        CASE
            WHEN o.order_status = 'delivered'
                 AND o.order_purchase_timestamp IS NOT NULL
                 AND o.order_delivered_customer_date IS NOT NULL
            THEN julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)
            ELSE NULL
        END AS delivery_days
    FROM orders o
)
SELECT
    il.product_category_name_english AS category,
    ROUND(SUM(il.item_revenue), 2) AS total_revenue,
    ROUND(AVG(od.delivery_days), 2) AS avg_delivery_days
FROM item_level il
LEFT JOIN order_delivery od
    ON od.order_id = il.order_id
GROUP BY il.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 15;

-- ============================================================
-- 6) Order-level extract for Python A/B analysis
-- ============================================================
WITH payment_ranked AS (
    SELECT
        op.order_id,
        op.payment_type,
        COALESCE(op.payment_value, 0.0) AS payment_value,
        ROW_NUMBER() OVER (
            PARTITION BY op.order_id
            ORDER BY COALESCE(op.payment_value, 0.0) DESC, op.payment_sequential ASC
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
    c.customer_unique_id,
    COALESCE(c.customer_state, 'unknown') AS customer_state,
    date(o.order_purchase_timestamp) AS purchase_date,
    strftime('%Y-%m', o.order_purchase_timestamp) AS purchase_month,
    o.order_status,
    COALESCE(pa.payment_total, 0.0) AS payment_total,
    COALESCE(pp.payment_type, 'unknown') AS payment_type,
    CASE
        WHEN COALESCE(pa.payment_total, 0.0) > 0.0 AND o.order_approved_at IS NOT NULL THEN 1
        ELSE 0
    END AS is_paid,
    CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_delivered
FROM orders o
LEFT JOIN customers c
    ON c.customer_id = o.customer_id
LEFT JOIN payment_agg pa
    ON pa.order_id = o.order_id
LEFT JOIN payment_primary pp
    ON pp.order_id = o.order_id;
