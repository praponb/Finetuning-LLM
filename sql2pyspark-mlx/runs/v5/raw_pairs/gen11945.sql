WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    CASE WHEN tier_value >= 73 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_tier,
    currency,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    SUM(CASE WHEN currency = 'THB' THEN tier_value ELSE 0 END) AS thb_tier_value,
    MAX(tier_value) AS max_tier_value,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count_2,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from) ORDER BY COUNT(CASE WHEN currency = 'EUR' THEN 1 END) DESC) AS eur_count_percent_rank
FROM lines
GROUP BY DATE_TRUNC('YEAR', effective_from), CASE WHEN tier_value >= 73 THEN 'LARGE' ELSE 'SMALL' END, currency
HAVING max_tier_value > 0
ORDER BY eur_count_2 DESC NULLS FIRST
