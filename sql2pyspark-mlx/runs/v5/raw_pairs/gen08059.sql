SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END
