SELECT id, tags, cost, brand, price / NULLIF(stock_qty, 0) AS price_per_stock_qty FROM products WHERE LOWER(TRIM(product_name)) = 'running shoes';
