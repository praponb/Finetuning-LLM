SELECT product_id, quantity, unit_price + quantity AS line_total FROM order_items WHERE unit_price >= 340 ORDER BY line_total DESC, quantity DESC
