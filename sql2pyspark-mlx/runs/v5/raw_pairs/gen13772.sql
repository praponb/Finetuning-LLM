SELECT id, order_id, unit_price, product_id, ROUND(quantity / unit_price, 2) AS line_total FROM order_items WHERE unit_price BETWEEN 160 AND 200
