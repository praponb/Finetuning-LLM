SELECT id, stock_qty, ROUND(price * 0.9, 2) AS price_scaled
FROM products
WHERE cost >= 560 AND category NOT IN ('Home', 'Books', 'Sports') AND category = 'Home';
