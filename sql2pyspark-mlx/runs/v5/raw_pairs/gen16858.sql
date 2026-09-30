SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(p.stock_qty) AS min_stock_qty
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END
