SELECT supplier_id, SUM(stock_qty) AS total_stock_qty, SUM(SUM(stock_qty)) OVER (ORDER BY supplier_id) AS cumulative_stock_qty
FROM products
GROUP BY supplier_id
