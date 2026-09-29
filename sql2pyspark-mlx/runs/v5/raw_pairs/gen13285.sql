select id, last_restock, store_id from inventory where product_id = 8 except select id, last_restock, store_id from inventory where product_id in (1, 2, 8, 10);
