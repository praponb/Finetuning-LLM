SELECT id, category, stock_qty, RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC) AS rnk FROM products
