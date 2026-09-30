WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        pos + 1 AS position,
        item.qty,
        item.price,
        item.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, item
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        qty,
        price,
        tag
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) e AS tag
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM exploded_carts
    EXCEPT DISTINCT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded_carts s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(qty) AS qty_whole,
        CEIL(qty) AS qty_whole_2
    FROM reconciled
    WHERE tag IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        qty,
        price,
        tag,
        qty_whole,
        qty_whole_2,
        SUM(qty_whole_2) OVER (PARTITION BY tag) AS tag_sum_qty_whole_2,
        ROUND(price / SUM(price) OVER (PARTITION BY tag), 4) AS price_share
    FROM cleaned
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN price_share >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_share_tier,
        PERCENTILE_APPROX(price_share, 0.5) AS price_share_p50,
        MAX(qty_whole) FILTER (WHERE tag = 'VIP') AS vip_max_qty_whole,
        SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(qty, ARRAY(0.25, 0.5, 0.75)) AS qty_quartiles,
        GROUPING_ID() AS grouping_level
    FROM with_windows
    GROUP BY CUBE (CASE WHEN price_share >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING vip_max_qty_whole < 1000000
)
SELECT
    price_share_tier,
    price_share_p50,
    tag_list,
    qty_quartiles,
    grouping_level
FROM stats
ORDER BY price_share_tier
