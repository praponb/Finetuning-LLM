SELECT product_id, order_id, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price FROM order_items
