SELECT s.country, MIN(p.stock_qty) AS min_stock_qty, ROUND(AVG(p.cost), 2) AS avg_cost
FROM products p
LEFT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY s.country
ORDER BY s.country
LIMIT 10
