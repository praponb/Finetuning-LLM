WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    ROUND(AVG(element), 2) AS avg_element,
    ROUND(AVG(element), 2) AS avg_element_2,
    ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS element_per_pro,
    MAX(element) AS max_element,
    SUM(CASE WHEN plan = 'starter' THEN element ELSE 0 END) AS starter_element
FROM flattened
GROUP BY DATE_TRUNC('WEEK', effective_from)
HAVING element_per_pro > 0;
