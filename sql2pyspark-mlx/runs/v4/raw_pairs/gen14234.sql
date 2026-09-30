SELECT inventory.id, inventory.last_restock, inventory.product_id, stores.sqft FROM inventory JOIN stores ON inventory.store_id = stores.id WHERE stores.region IS NULL
