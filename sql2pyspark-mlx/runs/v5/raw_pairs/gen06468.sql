select last_restock, product_id from inventory where quantity <= 68 except select last_restock, product_id from inventory where product_id in (1, 2, 10) order by last_restock;
