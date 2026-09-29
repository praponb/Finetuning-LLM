SELECT CASE WHEN s.rating >= 4.1 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, s.city, COUNT(i.quantity) AS quantity_count
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 4.1 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END, s.city
ORDER BY rating_bucket;
