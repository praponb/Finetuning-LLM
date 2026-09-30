SELECT id, stock_qty, RANK() OVER (ORDER BY stock_qty) AS rnk FROM products WHERE brand = 'Globex';
