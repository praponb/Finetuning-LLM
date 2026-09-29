SELECT id, unit_price, order_id, quantity, CAST(quantity AS STRING) AS quantity_num, CAST(quantity AS STRING) AS quantity_num_2 FROM order_items ORDER BY quantity_num_2 ASC, id ASC LIMIT 50
