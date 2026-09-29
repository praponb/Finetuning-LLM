select last_restock, quantity, store_id, date_trunc('month', last_restock) as last_restock_period from inventory;
