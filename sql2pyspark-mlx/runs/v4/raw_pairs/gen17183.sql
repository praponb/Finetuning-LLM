SELECT s.city, SUM(p.stock_qty) AS total_stock_qty
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating < 3.2
GROUP BY s.city
ORDER BY s.city ASC;
