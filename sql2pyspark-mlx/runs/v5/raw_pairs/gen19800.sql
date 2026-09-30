WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) li AS line
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        product_id,
        qty,
        price,
        tag
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS tag
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(tag, 1, 2) AS tag_prefix,
        COALESCE(tag, 'Unknown') AS tag_filled
    FROM flattened_carts
    WHERE tag IS NOT NULL
      AND price > 80
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        product_id,
        price,
        tag_prefix,
        tag_filled,
        qty + price AS qty_plus_price,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label
    FROM valid
    WHERE created_at >= DATE '2025-03-01'
      AND product_id <> 4
),
enriched AS (
    -- Ranking and gap features
    SELECT
        product_id,
        price,
        tag_prefix,
        tag_filled,
        created_at_label,
        PERCENT_RANK() OVER w_running AS price_percent_rank,
        MAX(price) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_max_1h
    FROM base
    WINDOW w_running AS (ORDER BY price NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN price >= 182 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    product_id,
    APPROX_PERCENTILE(price_max_1h, 0.9) AS price_max_1h_p90,
    ROUND(AVG(price_max_1h), 2) AS avg_price_max_1h
FROM enriched
GROUP BY 1, 2
