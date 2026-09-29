SELECT brand, ROUND(AVG(stock_qty), 2) avg_stock_qty FROM products GROUP BY brand HAVING ROUND(AVG(stock_qty), 2) > 24 ORDER BY brand DESC;
