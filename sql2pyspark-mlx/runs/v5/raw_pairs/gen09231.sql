WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DAY(effective_from) AS effective_from_day
    FROM flattened
    WHERE effective_from >= TO_DATE('2024-02-14')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY effective_from_day NULLS LAST) AS effective_from_day_dense_rank,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        RANK() OVER w_part AS tier_value_rank
    FROM cleaned
    WINDOW w_part AS (ORDER BY tier_value NULLS FIRST)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(tier_value_rank, effective_from_day) AS least_tier_value_rank_effective_from_day,
        LAST_DAY(effective_from) AS effective_from_month_end
    FROM metrics
    WHERE effective_from >= DATE '2024-05-14'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS tier_value_dense_rank_tier,
    MAX(tier_value_dense_rank) AS max_tier_value_dense_rank,
    ROUND(AVG(effective_from_day), 2) AS avg_effective_from_day,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    PERCENTILE_APPROX(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles_2,
    COUNT(*) AS row_count,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS grouping_level
FROM valid
GROUP BY ROLLUP (CASE WHEN tier_value_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END)
