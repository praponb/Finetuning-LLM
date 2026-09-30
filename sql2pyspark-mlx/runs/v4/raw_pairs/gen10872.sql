SELECT CASE WHEN p.price >= 1310 THEN 'high' WHEN p.price >= 450 THEN 'mid' ELSE 'low' END price_bucket, AVG(i.quantity) avg_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN stores s ON s.id = i.store_id
GROUP BY CASE WHEN p.price >= 1310 THEN 'high' WHEN p.price >= 450 THEN 'mid' ELSE 'low' END
