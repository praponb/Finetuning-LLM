SELECT CASE WHEN s.rating >= 3.5 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(STDDEV(p.price), 2) AS stddev_price
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.5 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END
HAVING ROUND(STDDEV(p.price), 2) > 22
ORDER BY stddev_price DESC, rating_bucket
LIMIT 5;
