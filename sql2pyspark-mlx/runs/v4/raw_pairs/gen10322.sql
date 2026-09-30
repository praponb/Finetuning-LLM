WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element
)
-- Grouped metrics
SELECT
    CASE WHEN element >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_tier,
    plan,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN plan = 'starter' THEN element ELSE 0 END) AS starter_element
FROM exploded
GROUP BY CASE WHEN element >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END, plan
ORDER BY element_tier DESC;
