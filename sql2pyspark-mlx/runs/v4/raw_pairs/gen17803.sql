SELECT id, order_id, unit_price, quantity, ROUND(quantity * 1.07, 2) AS adjusted_quantity, ROUND(unit_price, 2) AS unit_price_rounded FROM order_items WHERE product_id IN (10, 12, 17, 24);
