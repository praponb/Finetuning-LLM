WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) AS element_dense_rank,
        AVG(element) OVER (PARTITION BY plan) AS plan_avg_element,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY element) AS element_percent_rank
    FROM flattened
    WHERE element >= 30
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        element,
        plan_avg_element,
        plan_avg_element + element_dense_rank AS plan_avg_element_plus_element_dense_rank
    FROM with_windows
    WHERE element_dense_rank < 5
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN plan_avg_element_plus_element_dense_rank >= 66 THEN 'High' WHEN plan_avg_element_plus_element_dense_rank >= 18 THEN 'Medium' ELSE 'Low' END AS plan_avg_element_plus_element_dense_rank_band
    FROM base
    WHERE plan_avg_element IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN plan_avg_element_plus_element_dense_rank >= 68 THEN 'LARGE' ELSE 'SMALL' END AS plan_avg_element_plus_element_dense_rank_tier,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
FROM filtered
GROUP BY plan_avg_element_plus_element_dense_rank_tier
ORDER BY plan_avg_element_plus_element_dense_rank_tier;
