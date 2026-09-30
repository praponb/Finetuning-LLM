WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS element
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    plan,
    currency,
    CASE WHEN element >= 81 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
    AVG(element) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_element,
    COUNT_IF(currency = 'EUR') AS eur_events,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    APPROX_PERCENTILE(element, 0.95) AS element_p95,
    MAX(element) AS max_element
FROM exploded
GROUP BY plan, currency, element_tier
HAVING APPROX_PERCENTILE(element, 0.95) >= 0
ORDER BY currency DESC;
