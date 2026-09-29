SELECT CASE WHEN p.price >= 1040 THEN 'high' WHEN p.price >= 260 THEN 'mid' ELSE 'low' END AS price_bucket, SUM(ss.revenue) AS total_revenue, MAX(ss.revenue) AS max_revenue
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
LEFT JOIN products p ON ss.product_id = p.id
GROUP BY CASE WHEN p.price >= 1040 THEN 'high' WHEN p.price >= 260 THEN 'mid' ELSE 'low' END
ORDER BY total_revenue, price_bucket
LIMIT 5
