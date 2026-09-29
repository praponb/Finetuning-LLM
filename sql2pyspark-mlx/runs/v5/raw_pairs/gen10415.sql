select id, quantity, product_id, unit_price, round(quantity * 1.15, 2) as quantity_scaled, quantity + unit_price as line_total from order_items
