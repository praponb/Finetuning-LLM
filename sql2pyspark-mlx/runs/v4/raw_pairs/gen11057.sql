SELECT CASE WHEN s.rating >= 3.5 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.5 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END
HAVING ROUND(STDDEV(ss.revenue), 2) > 26
ORDER BY rating_bucket
LIMIT 1;
