SELECT category, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty, MIN(cost) AS min_cost FROM products GROUP BY category;
