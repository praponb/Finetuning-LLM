SELECT CASE WHEN p.cost >= 740 THEN 'high' WHEN p.cost >= 180 THEN 'mid' ELSE 'low' END AS cost_bucket, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN p.cost >= 740 THEN 'high' WHEN p.cost >= 180 THEN 'mid' ELSE 'low' END
ORDER BY avg_unit_price DESC
