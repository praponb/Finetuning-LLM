WITH supplier_stats AS (
    SELECT supplier_id, MAX(stock_qty) - MIN(stock_qty) stock_qty_range, AVG(cost) avg_cost
    FROM products
    WHERE product_name LIKE 'Wat%'
    GROUP BY supplier_id
)
SELECT s.city, ss.stock_qty_range, ss.avg_cost
FROM suppliers s
INNER JOIN supplier_stats ss ON s.id = ss.supplier_id;
