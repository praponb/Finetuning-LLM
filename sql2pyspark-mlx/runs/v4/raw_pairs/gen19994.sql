SELECT product_id, store_id, CAST(quantity AS STRING) AS quantity_str FROM inventory WHERE quantity <= 67;
