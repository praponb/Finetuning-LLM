select id, quantity, last_restock, store_id, date_format(last_restock, 'yyyy-MM') last_restock_label from inventory
