SELECT brand, MAX(price) max_price FROM products GROUP BY brand HAVING max_price > 1050;
