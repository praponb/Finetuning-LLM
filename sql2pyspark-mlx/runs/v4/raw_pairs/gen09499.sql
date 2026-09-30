select id, order_id, unit_price, quantity, quantity + unit_price as line_total, quantity - unit_price as line_total_2 from order_items where quantity = 6 and order_id = 20;
