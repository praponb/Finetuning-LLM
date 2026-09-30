WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM flattened
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE created_at >= '2023-08-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(product_id AS STRING) AS product_id_str,
        GREATEST(qty, price) AS greatest_qty_price,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM reconciled
    WHERE promo IS DISTINCT FROM 'SPRING10'
      AND promo IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(qty / price, 2) AS qty_per_price,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM valid
    WHERE created_at >= TIMESTAMP '2025-03-01 21:00:00'
)
-- Multi-level aggregation
SELECT
    product_id,
    promo,
    CASE WHEN greatest_qty_price >= 70 THEN 'HIGH' ELSE 'LOW' END AS greatest_qty_price_tier,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY product_id, promo, greatest_qty_price_tier
HAVING distinct_ids > 1
ORDER BY span_minutes;
