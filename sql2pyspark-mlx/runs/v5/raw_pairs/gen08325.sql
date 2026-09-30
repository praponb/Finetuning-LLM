select quantity, store_id, last_day(last_restock) as last_restock_month_end from inventory where quantity != 218;
