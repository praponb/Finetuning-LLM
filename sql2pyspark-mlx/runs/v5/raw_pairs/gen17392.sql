WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    id,
    plan,
    effective_from,
    CASE WHEN plan RLIKE '^(starter|pro)$' THEN 'OTHER_ZONE' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS plan_zone
FROM lines
ORDER BY plan_zone NULLS FIRST;
