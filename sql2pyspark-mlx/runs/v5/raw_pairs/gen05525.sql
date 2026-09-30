WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    MIN(element) AS min_element,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    SIZE(COLLECT_SET(currency)) AS num_currencies
FROM lines
GROUP BY DATE_TRUNC('YEAR', effective_from);
