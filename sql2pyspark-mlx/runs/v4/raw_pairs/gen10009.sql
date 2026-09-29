SELECT CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN p.category = 'Books' THEN p.stock_qty ELSE 0 END) AS books_stock_qty, AVG(p.stock_qty) AS avg_stock_qty
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE p.cost < 590
GROUP BY CASE WHEN s.rating >= 4.5 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END
ORDER BY avg_stock_qty;
