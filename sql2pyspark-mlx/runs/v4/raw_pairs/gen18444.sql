SELECT unit_price, quantity, COALESCE(quantity, 0) AS quantity_filled FROM order_items WHERE unit_price >= 220 ORDER BY quantity_filled DESC;
