WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN element IN ('BUNDLE', 'WELCOME5') THEN 'Tier 1' ELSE 'Other' END AS element_group,
        CASE WHEN element = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        COALESCE(element, 'NONE') AS element_filled
    FROM flattened
    WHERE element = 'BUNDLE'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        element,
        element_group,
        is_vip,
        element_filled,
        RANK() OVER (PARTITION BY element ORDER BY is_vip) AS is_vip_rank,
        MAX(is_vip) OVER w_hist AS element_max_is_vip,
        MIN(is_vip) OVER w_hist AS element_min_is_vip,
        MIN(is_vip) OVER w_hist AS element_min_is_vip_2,
        RANK() OVER (ORDER BY is_vip DESC) AS is_vip_rank_2
    FROM valid
    WINDOW w_hist AS (PARTITION BY element)
)
-- Aggregate with conditional metrics
SELECT
    element,
    APPROX_PERCENTILE(is_vip, 0.95) AS is_vip_p95,
    APPROX_PERCENTILE(is_vip_rank, ARRAY(0.25, 0.5, 0.75)) AS is_vip_rank_quartiles,
    GROUPING_ID() AS gid
FROM metrics
GROUP BY CUBE (element)
ORDER BY element NULLS LAST
