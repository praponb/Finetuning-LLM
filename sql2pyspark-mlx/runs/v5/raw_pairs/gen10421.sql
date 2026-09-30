SELECT id, product_id, GREATEST(quantity, unit_price) greatest_quantity_unit_price
FROM order_items
WHERE (order_id IN (13, 18, 22, 29) AND order_id = 17) OR order_id IN (2, 5, 6, 9)
