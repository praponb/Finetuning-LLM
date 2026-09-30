SELECT id, stock_qty, ROUND(price, 1) AS price_rounded FROM products WHERE brand IN ('Initech', 'Acme');
