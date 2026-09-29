SELECT category, SUM(stock_qty) AS total_stock_qty, SUM(SUM(stock_qty)) OVER (ORDER BY category) AS cumulative_stock_qty FROM products GROUP BY category
