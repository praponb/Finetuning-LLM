SELECT
    CASE WHEN units >= 40 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END AS units_bucket,
    YEAR(sale_date) AS sale_year,
    MIN(units) AS min_units,
    MIN(revenue) AS min_revenue,
    COUNT(*) AS total_count
FROM store_sales
WHERE units <= 15
GROUP BY CASE WHEN units >= 40 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END, YEAR(sale_date);
