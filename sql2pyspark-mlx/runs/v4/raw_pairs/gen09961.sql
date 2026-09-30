select id, unit_price, order_id, quantity + unit_price as line_total from order_items where unit_price between 320 and 330
