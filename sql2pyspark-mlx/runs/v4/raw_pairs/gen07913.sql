SELECT s.country, MAX(p.stock_qty) - MIN(p.stock_qty) AS stock_qty_range, MAX(p.launch_date) AS last_launch_date
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'bestseller')
GROUP BY s.country
ORDER BY s.country DESC
LIMIT 10
