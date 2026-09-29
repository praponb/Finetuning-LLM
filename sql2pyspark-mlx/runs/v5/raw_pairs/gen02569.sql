SELECT CASE WHEN revenue >= 3000 THEN 'high' WHEN revenue >= 1800 THEN 'mid' ELSE 'low' END AS revenue_bucket, product_id, SUM(revenue) AS total_revenue
FROM store_sales
GROUP BY revenue_bucket, product_id
