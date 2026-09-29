SELECT i.id, i.quantity, i.last_restock, i.product_id FROM inventory i WHERE i.product_id IN (7, 9) AND i.product_id NOT IN (SELECT id FROM products WHERE SIZE(tags) >= 2);
