WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
enriched AS (
    SELECT
        id,
        plan,
        DENSE_RANK() OVER (ORDER BY element) AS element_dense_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_percent_rank,
        MAX(element) OVER (PARTITION BY plan) AS plan_max_element,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS LAST) AS element_percent_rank_2,
        RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC NULLS FIRST) AS element_rank
    FROM exploded
),
with_windows AS (
    SELECT
        id,
        element_percent_rank_2,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY plan), 4) AS element_dense_rank_share,
        DENSE_RANK() OVER w_recent AS element_rank_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_rank DESC) AS element_rank_cume_dist,
        RANK() OVER (ORDER BY plan_max_element NULLS LAST) AS plan_max_element_rank,
        DENSE_RANK() OVER w_recent AS element_rank_dense_rank_2
    FROM enriched
    WINDOW w_recent AS (PARTITION BY plan ORDER BY element_rank DESC NULLS FIRST)
),
base AS (
    SELECT
        id,
        element_dense_rank_share,
        element_rank_dense_rank,
        element_rank_cume_dist,
        plan_max_element_rank,
        element_rank_dense_rank_2,
        CEIL(element_rank_dense_rank) AS element_rank_dense_rank_whole
    FROM with_windows
    WHERE element_percent_rank_2 > 0.61
),
base_price_tiers AS (
    SELECT *, ROUND(element_dense_rank_share, 2) AS element_dense_rank_share_rounded
    FROM base
    WHERE plan_max_element_rank = 5
),
valid AS (
    SELECT *, element_dense_rank_share / NULLIF(element_dense_rank_share_rounded, 0) AS element_dense_rank_share_per_element_dense_rank_share_rounded
    FROM base_price_tiers
    WHERE element_rank_dense_rank_2 > 3
)
SELECT
    CASE WHEN element_rank_dense_rank_whole >= 70 THEN 'HIGH' ELSE 'LOW' END AS element_rank_dense_rank_whole_tier,
    CASE WHEN element_dense_rank_share_per_element_dense_rank_share_rounded >= 80 THEN 'HIGH' ELSE 'LOW' END AS element_dense_rank_share_per_element_dense_rank_share_rounded_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(element_rank_dense_rank, 0.9) AS element_rank_dense_rank_p90,
    COUNT(*) AS row_count,
    GROUPING(CASE WHEN element_rank_dense_rank_whole >= 70 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN element_rank_dense_rank_whole >= 70 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN element_dense_rank_share_per_element_dense_rank_share_rounded >= 80 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN element_rank_dense_rank_whole >= 70 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY row_count DESC NULLS FIRST;
