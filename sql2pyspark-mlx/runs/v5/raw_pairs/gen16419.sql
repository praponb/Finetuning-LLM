SELECT id, order_id, product_id, CAST(quantity AS STRING) quantity_str, GREATEST(quantity, unit_price) greatest_quantity_unit_price FROM order_items WHERE quantity = 6
