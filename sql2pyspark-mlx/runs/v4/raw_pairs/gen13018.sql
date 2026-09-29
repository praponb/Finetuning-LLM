SELECT brand, category, AVG(stock_qty) AS avg_stock_qty FROM products GROUP BY brand, category HAVING AVG(stock_qty) < 250 ORDER BY category, brand LIMIT 3;
