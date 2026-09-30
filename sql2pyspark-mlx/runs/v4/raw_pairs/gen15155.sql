SELECT
    CASE WHEN revenue >= 4100 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    AVG(units) AS avg_units,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    SUM(units) AS total_units,
    APPROX_PERCENTILE(units, 0.95) AS units_p95,
    GROUPING_ID() AS gid
FROM store_sales
GROUP BY ROLLUP (CASE WHEN revenue >= 4100 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', sale_date))
ORDER BY total_units ASC
