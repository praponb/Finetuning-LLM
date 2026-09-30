SELECT CASE WHEN revenue >= 3800 THEN 'high' WHEN revenue >= 1600 THEN 'mid' ELSE 'low' END AS revenue_bucket, product_id, COUNT(revenue) AS revenue_count
FROM store_sales
WHERE units <= 12 AND revenue >= 3400
GROUP BY revenue_bucket, product_id
ORDER BY product_id;
