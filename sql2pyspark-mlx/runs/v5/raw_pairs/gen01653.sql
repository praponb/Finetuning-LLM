SELECT CASE WHEN revenue >= 3300 THEN 'high' WHEN revenue >= 800 THEN 'mid' ELSE 'low' END AS revenue_bucket, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MIN(sale_date) AS first_sale_date
FROM store_sales
WHERE product_id IN (1, 2, 6) AND (units < 12 OR product_id IN (3, 4, 8))
GROUP BY CASE WHEN revenue >= 3300 THEN 'high' WHEN revenue >= 800 THEN 'mid' ELSE 'low' END, DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY sale_month, revenue_bucket;
