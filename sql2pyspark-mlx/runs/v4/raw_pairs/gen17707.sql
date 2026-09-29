SELECT CASE WHEN units >= 36 THEN 'high' WHEN units >= 15 THEN 'mid' ELSE 'low' END AS units_bucket, AVG(revenue) AS avg_revenue
FROM store_sales
WHERE sale_date BETWEEN '2023-06-03' AND '2025-09-01'
GROUP BY 1
HAVING avg_revenue >= 2300;
