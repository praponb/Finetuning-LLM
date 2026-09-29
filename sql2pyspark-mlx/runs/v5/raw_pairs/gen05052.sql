WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos + 1 AS item_no,
        line.product_id,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, line
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(product_id) OVER (PARTITION BY promo) AS promo_avg_product_id,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS FIRST) AS price_dense_rank,
        COUNT(product_id) OVER (PARTITION BY promo) AS promo_count_product_id
    FROM exploded
    WHERE product_id BETWEEN 7 AND 9
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        coupon_codes,
        item_no,
        product_id,
        price,
        promo_avg_product_id,
        promo_count_product_id,
        COUNT(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_count_15m,
        MIN(item_no) OVER (PARTITION BY promo) AS promo_min_item_no
    FROM scored
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN promo_min_item_no >= 3 THEN 'High' WHEN promo_min_item_no >= 1 THEN 'Medium' ELSE 'Low' END AS promo_min_item_no_level,
        ROUND(item_no, 1) AS item_no_rounded
    FROM scored_carts
    WHERE created_at >= DATE '2025-03-01'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('DAY', created_at) AS activity_day,
        ROUND(AVG(promo_min_item_no), 2) AS avg_promo_min_item_no,
        APPROX_PERCENTILE(item_no_rounded, 0.9) AS item_no_rounded_p90,
        MIN(created_at) AS first_created_at,
        PERCENTILE_APPROX(promo_count_product_id, 0.9) AS promo_count_product_id_p90
    FROM verified
    GROUP BY 1
)
SELECT
    activity_day,
    avg_promo_min_item_no,
    item_no_rounded_p90,
    promo_count_product_id_p90
FROM grouped
ORDER BY item_no_rounded_p90
