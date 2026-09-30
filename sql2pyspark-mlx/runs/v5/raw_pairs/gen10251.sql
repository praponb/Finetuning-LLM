WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
)
-- Grouped metrics
SELECT
    currency,
    ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS element_per_starter,
    MIN(element) AS min_element,
    COUNT(DISTINCT plan) AS distinct_plans,
    MAX(element) AS max_element,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows
FROM flattened
GROUP BY currency
HAVING thb_rows >= 1
ORDER BY max_element;
