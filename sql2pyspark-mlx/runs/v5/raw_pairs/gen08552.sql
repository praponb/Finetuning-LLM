SELECT s.country, ROUND(AVG(p.stock_qty), 2) AS avg_stock_qty
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY s.country;
