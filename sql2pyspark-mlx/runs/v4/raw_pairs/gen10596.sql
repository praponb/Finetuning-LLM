SELECT store_id, ADD_MONTHS(last_restock, 1) AS last_restock_plus_months, ROUND(quantity * 1.15, 2) AS quantity_adjusted FROM inventory
