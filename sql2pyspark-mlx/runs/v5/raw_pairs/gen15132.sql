WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value >= 85 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    SIZE(COLLECT_SET(currency)) AS num_currencies
FROM lines
GROUP BY CASE WHEN tier_value >= 85 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_currencies
