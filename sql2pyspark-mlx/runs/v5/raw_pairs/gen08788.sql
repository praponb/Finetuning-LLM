SELECT i.id, i.quantity, i.last_restock, i.product_id FROM inventory i WHERE i.store_id NOT IN (SELECT id FROM stores WHERE region = 'North')
