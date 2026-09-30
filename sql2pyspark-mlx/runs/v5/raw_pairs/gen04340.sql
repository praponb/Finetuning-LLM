WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank,
        CUME_DIST() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank,
        DENSE_RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_dense_rank
    FROM lines
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(effective_from, 12) AS effective_from_plus_months,
        ROUND(tier_value * 1.15, 2) AS adjusted_tier_value
    FROM with_windows
    WHERE effective_from >= TO_DATE('2024-03-19')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        tier_value_rank,
        tier_value_dense_rank,
        adjusted_tier_value,
        ROUND(tier_value_percent_rank, 2) AS tier_value_percent_rank_rounded
    FROM cleaned
    WHERE effective_from > DATE '2024-02-18'
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        tier_value_percent_rank_rounded,
        CUME_DIST() OVER (ORDER BY adjusted_tier_value DESC NULLS LAST) AS adjusted_tier_value_cume_dist,
        RANK() OVER (ORDER BY tier_value_dense_rank) AS tier_value_dense_rank_rank
    FROM valid
    WHERE tier_value_percent_rank_rounded < 73
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_tier_value_cume_dist >= 0.71 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_tier_value_cume_dist_tier,
    PERCENTILE_APPROX(tier_value_dense_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS tier_value_dense_rank_rank_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(tier_value_dense_rank_rank) AS total_tier_value_dense_rank_rank,
    PERCENTILE_APPROX(adjusted_tier_value_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS adjusted_tier_value_cume_dist_quartiles
FROM with_windows_price_tiers
GROUP BY CASE WHEN adjusted_tier_value_cume_dist >= 0.71 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(DISTINCT id) > 1;
