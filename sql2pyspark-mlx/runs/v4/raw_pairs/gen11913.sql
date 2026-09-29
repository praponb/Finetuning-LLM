SELECT i.id, i.last_restock, i.store_id, i.quantity FROM inventory i WHERE i.product_id NOT IN (SELECT id FROM products WHERE price <= 1150)
