SELECT inventory.id, inventory.last_restock, inventory.store_id, inventory.product_id FROM inventory WHERE inventory.store_id IN (SELECT id FROM stores WHERE sqft <= 11933);
