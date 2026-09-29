SELECT s.country, CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(*) AS cnt
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country, CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END;
