SELECT *, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity, quantity * unit_price AS line_total FROM order_items WHERE product_id IN (1, 10, 21, 39);
