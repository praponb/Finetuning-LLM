SELECT category, SUM(stock_qty) total_stock_qty, ROUND(SUM(stock_qty) * 100.0 / SUM(SUM(stock_qty)) OVER (), 2) AS pct_of_total FROM products GROUP BY category;
