SELECT CASE WHEN p.cost >= 720 THEN 'high' WHEN p.cost >= 130 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(oi.quantity) AS quantity_count
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost > 200
GROUP BY CASE WHEN p.cost >= 720 THEN 'high' WHEN p.cost >= 130 THEN 'mid' ELSE 'low' END;
