SELECT CASE WHEN p.cost >= 630 THEN 'high' WHEN p.cost >= 210 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(*) AS num_store_sales, MIN(ss.sale_date) AS min_sale_date
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
GROUP BY CASE WHEN p.cost >= 630 THEN 'high' WHEN p.cost >= 210 THEN 'mid' ELSE 'low' END
ORDER BY 2 DESC
