SELECT
    CASE WHEN units >= 43 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(units), 2) AS avg_units,
    CORR(units, revenue) AS corr_units_revenue,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_2,
    MIN(units) AS min_units
FROM store_sales
GROUP BY GROUPING SETS ((CASE WHEN units >= 43 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', sale_date)), (CASE WHEN units >= 43 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY distinct_ids_2 NULLS FIRST;
