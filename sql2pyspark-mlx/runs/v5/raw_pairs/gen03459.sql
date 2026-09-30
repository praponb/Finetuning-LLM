WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS tier_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank_2
    FROM lines
),
with_windows_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank_3,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY tier_value) AS tier_value_rank,
        CUME_DIST() OVER w_grp AS tier_value_cume_dist_cume_dist
    FROM with_windows_price_tiers
    WINDOW w_grp AS (ORDER BY tier_value_cume_dist DESC NULLS LAST)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        tier_value_dense_rank + tier_value AS tier_value_dense_rank_plus_tier_value,
        DATE_FORMAT(effective_from, 'yyyy-MM-dd') AS effective_from_label
    FROM metrics
    WHERE tier_value_dense_rank <> 6
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(tier_value_dense_rank_2 AS DOUBLE) AS tier_value_dense_rank_2_num,
        ROUND(tier_value_rank * 0.85, 2) AS tier_value_rank_adjusted
    FROM valid
    WHERE effective_from >= DATE '2024-03-04'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_dense_rank_2 DESC NULLS FIRST) AS tier_value_dense_rank_2_cume_dist,
        PERCENT_RANK() OVER (ORDER BY tier_value_dense_rank_plus_tier_value NULLS LAST) AS tier_value_dense_rank_plus_tier_value_percent_rank
    FROM base
    WHERE effective_from < '2024-12-12'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    PERCENTILE_APPROX(tier_value_dense_rank_2, 0.9) AS tier_value_dense_rank_2_p90,
    PERCENTILE_APPROX(tier_value_rank_adjusted, ARRAY(0.25, 0.5, 0.75)) AS tier_value_rank_adjusted_quartiles,
    COUNT(*) AS row_count,
    ROUND(AVG(tier_value_dense_rank_2_cume_dist), 2) AS avg_tier_value_dense_rank_2_cume_dist,
    PERCENTILE_APPROX(tier_value_dense_rank_3, 0.5) AS tier_value_dense_rank_3_p50,
    PERCENTILE_APPROX(tier_value_dense_rank_2_num, ARRAY(0.25, 0.5, 0.75)) AS tier_value_dense_rank_2_num_quartiles
FROM scored
GROUP BY 1;
