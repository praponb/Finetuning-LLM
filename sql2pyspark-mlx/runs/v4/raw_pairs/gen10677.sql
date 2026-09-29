SELECT s.city, COUNT(DISTINCT p.stock_qty) unique_suppliers
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city
ORDER BY unique_suppliers DESC, s.city DESC
LIMIT 5
