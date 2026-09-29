SELECT id, product_id, quantity, order_id, CAST(unit_price AS INT) AS unit_price_int, ROUND(quantity / unit_price, 2) AS line_total FROM order_items WHERE unit_price > 370;
