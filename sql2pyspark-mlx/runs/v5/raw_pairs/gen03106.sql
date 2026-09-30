WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos + 1 AS line_no,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, line
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        promo,
        code
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM flattened_carts
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened_carts s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(code) code_lower
    FROM reconciled
    WHERE code IS DISTINCT FROM 'BUNDLE'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    promo,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    COUNT_IF(promo = 'VIP') AS vip_events,
    GROUPING_ID() AS grouping_level
FROM valid
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), promo), (DATE_TRUNC('DAY', created_at)))
