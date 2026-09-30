WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, QUARTER(effective_from) AS effective_from_quarter
    FROM exploded
    WHERE tier_value <= 52
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        COUNT(effective_from_quarter) OVER (PARTITION BY plan) AS plan_count_effective_from_quarter,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY effective_from_quarter NULLS FIRST) AS effective_from_quarter_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_dense_rank
    FROM filtered
    WHERE effective_from_quarter >= 61
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS effective_from_quarter_percent_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY effective_from_quarter_percent_rank DESC) AS effective_from_quarter_percent_rank_percent_rank
    FROM metrics
    WINDOW w_hist AS (PARTITION BY plan ORDER BY effective_from_quarter_percent_rank DESC NULLS LAST)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(tier_value_dense_rank) OVER (PARTITION BY plan) AS plan_min_tier_value_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY effective_from_quarter_percent_rank DESC NULLS LAST) AS effective_from_quarter_percent_rank_cume_dist
    FROM metrics_price_tiers
)
-- Grouped metrics
SELECT
    plan,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
FROM with_windows
GROUP BY 1
