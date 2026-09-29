SELECT p.price, p.stock_qty, i.product_id FROM products p LEFT JOIN inventory i ON i.product_id = p.id WHERE i.store_id IN (1, 3, 4)
