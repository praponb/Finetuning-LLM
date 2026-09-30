SELECT category, COUNT(stock_qty) AS stock_qty_count, MAX(price) - MIN(price) AS price_range FROM products GROUP BY category;
