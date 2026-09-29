SELECT id, unit_price, ROUND(quantity, 0) AS quantity_rounded FROM order_items WHERE product_id IS NULL AND product_id IN (3, 27, 36, 38) AND product_id = 32
