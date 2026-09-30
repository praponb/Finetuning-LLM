WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
)
-- Window metrics over several different partitions
SELECT
    *,
    COUNT(tier_value) OVER w_running AS plan_count_tier_value,
    AVG(tier_value) OVER w_running AS plan_avg_tier_value,
    AVG(tier_value) OVER w_running AS plan_avg_tier_value_2
FROM lines
WINDOW w_running AS (PARTITION BY plan)
