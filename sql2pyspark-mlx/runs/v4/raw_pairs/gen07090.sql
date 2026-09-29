SELECT id, quantity, ADD_MONTHS(last_restock, 1) AS last_restock_plus_months FROM inventory WHERE product_id IN (8, 9) OR store_id IN (1, 2, 3) ORDER BY last_restock_plus_months DESC
