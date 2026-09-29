WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        item.product_id,
        item.qty,
        item.promo
    FROM carts t
    LATERAL VIEW OUTER EXPLODE(t.items) e AS item
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        product_id,
        promo,
        TRIM(promo) AS clean_promo,
        COALESCE(promo, 'DIRECT') AS promo_filled
    FROM lines
    WHERE customer_id IS NOT NULL
      AND customer_id IN (16, 17, 27, 40)
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        coupon_codes,
        product_id,
        clean_promo,
        promo_filled,
        DATE_TRUNC('month', created_at) AS created_at_period
    FROM base
    WHERE SIZE(coupon_codes) >= 2
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM base_carts
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM carts
    WHERE created_at >= '2024-07-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_carts s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    DATE_TRUNC('MONTH', created_at_period) AS activity_month,
    MIN(created_at_period) AS first_created_at_period,
    MIN(created_at_period) AS started_at,
    ROUND(AVG(product_id), 2) AS avg_product_id
FROM reconciled
GROUP BY 1, 2
HAVING ROUND(AVG(product_id), 2) > 0
ORDER BY product_id DESC
