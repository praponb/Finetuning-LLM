WITH flattened AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
)
SELECT
    CASE WHEN tier_value >= 87 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    COUNT(*) AS cnt,
    SUM(tier_value) AS total_tier_value,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count
FROM flattened
GROUP BY tier_value_tier
ORDER BY tier_value_tier NULLS LAST
