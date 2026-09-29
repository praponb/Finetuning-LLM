SELECT s.city, s.supplier_name, t.avg_stock_qty
FROM suppliers s
INNER JOIN (SELECT supplier_id, ROUND(AVG(stock_qty), 2) AS avg_stock_qty
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY s.city;
