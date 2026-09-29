SELECT
    CASE WHEN revenue >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    store_id,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(units) AS units_bit_and,
    GROUPING(CASE WHEN revenue >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM store_sales
GROUP BY ROLLUP (CASE WHEN revenue >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END, store_id)
HAVING distinct_ids > 1
ORDER BY distinct_ids;
