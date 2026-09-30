SELECT id, brand, category, cost, ROUND(stock_qty * 1.1, 2) AS stock_qty_adjusted FROM products ORDER BY cost, brand DESC, id LIMIT 5;
