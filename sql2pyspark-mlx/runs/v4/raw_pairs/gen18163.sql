SELECT CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, s.city, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END, s.city
