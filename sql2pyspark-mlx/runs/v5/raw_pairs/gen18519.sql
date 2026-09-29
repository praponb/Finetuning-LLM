select id, product_id, last_restock, store_id from inventory where month(last_restock) in (3, 8, 9) and quantity <= 114 and quantity > 160;
