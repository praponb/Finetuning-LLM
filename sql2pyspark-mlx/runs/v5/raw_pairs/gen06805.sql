WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, line
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        product_id,
        qty,
        price,
        promo,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) x AS element
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY qty DESC NULLS FIRST) AS qty_percent_rank,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank
    FROM exploded
    WHERE qty = 3
),
scored AS (
    -- Running and moving aggregates
    SELECT
        created_at,
        product_id,
        qty_percent_rank,
        product_id_dense_rank,
        COUNT(price) OVER (PARTITION BY promo) AS promo_count_price
    FROM with_windows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        product_id,
        product_id_dense_rank,
        promo_count_price,
        TO_DATE(created_at) AS created_at_date
    FROM scored
    WHERE qty_percent_rank > 0.52
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(promo_count_price / product_id, 2) AS promo_count_price_per_product_id
    FROM valid
    WHERE created_at >= '2024-02-15'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN product_id >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_tier,
    product_id,
    CASE WHEN promo_count_price_per_product_id >= 89 THEN 'LARGE' ELSE 'SMALL' END AS promo_count_price_per_product_id_tier,
    MIN(created_at) AS first_created_at,
    APPROX_PERCENTILE(promo_count_price, ARRAY(0.25, 0.5, 0.75)) AS promo_count_price_quartiles,
    MAX(created_at) AS last_created_at,
    COUNT(*) AS cnt
FROM base
GROUP BY ROLLUP (CASE WHEN product_id >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id, CASE WHEN promo_count_price_per_product_id >= 89 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY product_id_tier DESC NULLS LAST;
