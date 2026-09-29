SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, AVG(p.price) AS avg_price
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 1.5 THEN 'mid' ELSE 'low' END
ORDER BY avg_price DESC;
