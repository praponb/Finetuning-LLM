select id, last_restock, store_id, quantity from inventory where last_restock > '2023-12-20' order by id, last_restock;
