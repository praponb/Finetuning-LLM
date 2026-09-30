SELECT s.country, COUNT(p.id) AS product_count, COUNT(p.stock_qty) AS stock_qty_count
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE s.city IN ('Berlin', 'Chiang Mai')
GROUP BY s.country;
