select id, last_restock, quantity, product_id, date_format(last_restock, 'yyyy-MM-dd') as last_restock_label from inventory where product_id = 1 and product_id = 8 order by id desc;
