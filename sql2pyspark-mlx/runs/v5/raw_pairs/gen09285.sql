SELECT last_restock, store_id, product_id, ADD_MONTHS(last_restock, 6) AS last_restock_plus_months FROM inventory WHERE quantity = 155
