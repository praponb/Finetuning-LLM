WITH exploded AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS tier_value
)
SELECT
    plan,
    tier_value,
    CASE WHEN plan RLIKE '^(pro|enterprise)$' THEN 'PRIORITY' WHEN plan RLIKE '^(starter|basic)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS plan_zone
FROM exploded
ORDER BY plan_zone
