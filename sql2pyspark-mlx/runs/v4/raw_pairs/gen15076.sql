SELECT COUNT(quantity) AS quantity_count FROM order_items WHERE (order_id IN (9, 15, 20, 25) AND quantity = 6) OR quantity BETWEEN 4 AND 6
