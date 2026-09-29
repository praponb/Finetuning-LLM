SELECT id, order_id, unit_price, COALESCE(unit_price, 0) AS unit_price_filled FROM order_items WHERE unit_price >= 290
