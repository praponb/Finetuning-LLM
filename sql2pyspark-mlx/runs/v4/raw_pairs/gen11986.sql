SELECT CASE WHEN revenue >= 4200 THEN 'high' WHEN revenue >= 1100 THEN 'mid' ELSE 'low' END AS revenue_bucket, MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
GROUP BY 1
ORDER BY revenue_bucket DESC
