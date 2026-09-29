SELECT id, store_id, last_restock, product_id, CAST(quantity AS STRING) AS quantity_str FROM inventory WHERE quantity BETWEEN 168 AND 220 AND quantity < 148
