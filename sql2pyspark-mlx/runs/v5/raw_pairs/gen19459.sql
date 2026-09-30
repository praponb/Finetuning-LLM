SELECT inventory.id, inventory.store_id, inventory.quantity, inventory.product_id FROM inventory WHERE inventory.product_id NOT IN (SELECT id FROM products WHERE supplier_id IN (7, 8, 12, 36));
