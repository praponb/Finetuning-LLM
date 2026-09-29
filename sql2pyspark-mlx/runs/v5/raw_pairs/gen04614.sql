select inventory.id, inventory.last_restock, inventory.product_id, inventory.store_id from inventory where inventory.store_id not in (select id from stores where upper(store_name) like '%ALL%');
