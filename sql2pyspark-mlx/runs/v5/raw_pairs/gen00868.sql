WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS tier_value
)
SELECT
    id,
    plan,
    effective_from,
    currency,
    CASE WHEN plan RLIKE '^(enterprise|starter)$' THEN 'GROUP_B' WHEN plan RLIKE '^(basic|pro)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS plan_zone
FROM flattened
ORDER BY currency NULLS LAST;
