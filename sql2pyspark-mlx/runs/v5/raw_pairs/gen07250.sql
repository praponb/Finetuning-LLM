SELECT products.id, products.brand, products.stock_qty, products.product_name FROM products WHERE products.id IN (SELECT product_id FROM inventory)
