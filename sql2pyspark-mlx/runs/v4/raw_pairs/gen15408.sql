select id, order_id, unit_price + quantity line_total, coalesce(quantity, 0) quantity_filled from order_items where quantity <= 7 and (order_id in (4, 8) or order_id in (14, 28, 36));
