SELECT *, DATE_ADD(last_restock, 75) last_restock_plus_75d, ADD_MONTHS(last_restock, 3) last_restock_plus_months
FROM inventory
WHERE store_id = 2 AND store_id = 4;
