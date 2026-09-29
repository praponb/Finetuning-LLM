SELECT id, store_id, LAST_DAY(last_restock) AS last_restock_month_end, DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label FROM inventory ORDER BY last_restock_month_end DESC;
