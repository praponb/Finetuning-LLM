SELECT inventory.id, inventory.quantity, stores.store_name FROM inventory JOIN stores ON inventory.store_id = stores.id
