WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    plan,
    SUM(element) OVER (PARTITION BY plan) AS plan_sum_element,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC) AS element_cume_dist,
    MAX(element) OVER (PARTITION BY plan) AS plan_max_element
FROM lines
