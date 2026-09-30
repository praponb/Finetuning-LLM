SELECT s.city, AVG(p.stock_qty) avg_stock_qty
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city;
