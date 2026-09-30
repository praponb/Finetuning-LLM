WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS element
)
-- Multi-level aggregation
SELECT
    CASE WHEN element >= 75 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    plan,
    COUNT(DISTINCT plan) AS distinct_plans,
    ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS element_per_starter,
    ROUND(AVG(element), 2) AS avg_element,
    PERCENTILE_APPROX(element, 0.9) AS element_p90,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count
FROM lines
GROUP BY CASE WHEN element >= 75 THEN 'LARGE' ELSE 'SMALL' END, plan
HAVING ROUND(AVG(element), 2) < 0
ORDER BY avg_element DESC;
