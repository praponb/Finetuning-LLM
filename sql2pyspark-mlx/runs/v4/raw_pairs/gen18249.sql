select quantity, last_restock, product_id, date_format(last_restock, 'MMM yyyy') as last_restock_label from inventory where product_id in (1, 3, 4, 8) and store_id in (4, 5, 6) and product_id = 5
