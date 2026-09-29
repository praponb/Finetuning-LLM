SELECT
    QUARTER(sale_date) AS sale_quarter,
    CASE WHEN revenue >= 4000 THEN 'high' WHEN revenue >= 1500 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    AVG(revenue) AS avg_revenue,
    COUNT(units) AS units_count
FROM store_sales
WHERE revenue > 1400 AND store_id = 2
GROUP BY 1, 2
ORDER BY revenue_bucket DESC;
