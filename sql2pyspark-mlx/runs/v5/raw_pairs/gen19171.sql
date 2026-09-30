SELECT id, store_id, LAST_DAY(last_restock) AS last_restock_month_end FROM inventory;
