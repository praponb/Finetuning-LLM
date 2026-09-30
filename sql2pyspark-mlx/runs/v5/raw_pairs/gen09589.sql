WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    plan,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_percent_rank
FROM lines
WHERE plan IN ('enterprise', 'basic');
