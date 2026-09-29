SELECT CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 1.7 THEN 'mid' ELSE 'low' END;
