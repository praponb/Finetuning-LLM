WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS element
)
-- Grouped metrics
SELECT
    CASE WHEN element >= 63 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    COUNT_IF(currency = 'USD') AS usd_events,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    AVG(element) FILTER (WHERE currency = 'THB') AS thb_avg_element
FROM lines
GROUP BY CUBE (CASE WHEN element >= 63 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY element_tier DESC NULLS LAST;
