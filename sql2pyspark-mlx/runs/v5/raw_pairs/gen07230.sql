WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    currency,
    SUM(tier_value) FILTER (WHERE currency = 'THB') AS thb_total_tier_value,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    ROUND(AVG(tier_value), 2) AS avg_tier_value_2
FROM exploded
GROUP BY 1
ORDER BY thb_total_tier_value NULLS FIRST
