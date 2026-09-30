SELECT id, store_id, DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label, DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label_2 FROM inventory WHERE quantity = 107;
