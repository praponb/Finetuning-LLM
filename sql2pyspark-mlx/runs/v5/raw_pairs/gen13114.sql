SELECT
    store_id,
    CASE WHEN units >= 36 THEN 'high' WHEN units >= 17 THEN 'mid' ELSE 'low' END AS units_bucket,
    AVG(revenue) AS avg_revenue,
    SUM(revenue) AS total_revenue,
    MIN(units) AS min_units
FROM store_sales
GROUP BY store_id, units_bucket
ORDER BY 5 DESC;
