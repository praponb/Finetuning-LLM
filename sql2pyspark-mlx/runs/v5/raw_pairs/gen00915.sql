SELECT id, quantity, ROUND(quantity / unit_price, 2) AS line_total, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity FROM order_items WHERE quantity <> 7 AND unit_price >= 290
