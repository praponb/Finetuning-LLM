SELECT id, quantity, store_id, last_restock, CAST(quantity AS STRING) AS quantity_str, MONTH(last_restock) AS last_restock_month FROM inventory ORDER BY last_restock_month DESC, quantity
