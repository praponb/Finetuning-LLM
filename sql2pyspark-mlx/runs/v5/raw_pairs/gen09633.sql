SELECT CASE WHEN units >= 38 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END AS units_bucket, MAX(revenue) - MIN(revenue) AS revenue_range, MAX(revenue) AS max_revenue
FROM store_sales
WHERE revenue BETWEEN 1400 AND 2600 AND (product_id IN (1, 8) OR revenue > 2100)
GROUP BY CASE WHEN units >= 38 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END
HAVING SUM(revenue) < 15000
