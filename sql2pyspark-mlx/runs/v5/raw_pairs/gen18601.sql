SELECT id, store_id, ADD_MONTHS(last_restock, 12) last_restock_plus_months, CAST(quantity AS DOUBLE) quantity_str FROM inventory WHERE quantity IS NULL ORDER BY store_id, id LIMIT 3;
