WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        tag
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN k = 'device' THEN 1 ELSE 0 END AS is_device,
        CASE WHEN tag IN ('VIP', 'SPRING10') THEN 'Group A' ELSE 'Other' END AS tag_group
    FROM exploded
    WHERE tag IS NOT NULL
      AND k IS DISTINCT FROM 'gift'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY is_device DESC NULLS FIRST) AS is_device_dense_rank,
        DENSE_RANK() OVER (PARTITION BY tag, k ORDER BY is_device DESC NULLS LAST) AS is_device_dense_rank_2,
        MIN(is_device) OVER (PARTITION BY tag) AS tag_min_is_device,
        CUME_DIST() OVER w_recent AS is_device_cume_dist
    FROM filtered
    WINDOW w_recent AS (PARTITION BY k ORDER BY is_device NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        is_device_dense_rank - is_device AS is_device_dense_rank_minus_is_device,
        COALESCE(is_device_dense_rank, 0) AS is_device_dense_rank_filled
    FROM enriched
    WHERE is_device_dense_rank_2 < 6
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_device_dense_rank_2 >= 8 THEN 'HIGH' ELSE 'LOW' END AS is_device_dense_rank_2_tier,
    CASE WHEN is_device_dense_rank_filled >= 89 THEN 'LARGE' ELSE 'SMALL' END AS is_device_dense_rank_filled_tier,
    k,
    ROUND(SUM(tag_min_is_device) / NULLIF(COUNT(CASE WHEN tag = 'WELCOME5' THEN 1 END), 0), 2) AS tag_min_is_device_per_welcome5,
    MAX(is_device) AS max_is_device,
    SUM(CASE WHEN k = 'device' THEN is_device_dense_rank_2 ELSE 0 END) AS device_is_device_dense_rank_2,
    SIZE(COLLECT_SET(tag)) AS num_tags,
    MIN(is_device_dense_rank) AS min_is_device_dense_rank,
    SUM(CASE WHEN tag = 'WELCOME5' THEN is_device_dense_rank_filled ELSE 0 END) AS welcome5_is_device_dense_rank_filled
FROM valid
GROUP BY 1, 2, 3
HAVING SUM(CASE WHEN k = 'device' THEN is_device_dense_rank_2 ELSE 0 END) > 0;
