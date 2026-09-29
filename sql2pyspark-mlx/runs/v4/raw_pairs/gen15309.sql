SELECT brand, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty FROM products GROUP BY brand;
