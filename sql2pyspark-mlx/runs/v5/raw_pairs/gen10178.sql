WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(DISTINCT currency) AS distinct_currencies,
    ROUND(AVG(element), 2) AS avg_element,
    ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS element_per_eur
FROM lines
GROUP BY 1
HAVING ROUND(AVG(element), 2) > 0
ORDER BY distinct_currencies DESC;
