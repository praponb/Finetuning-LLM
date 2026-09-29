SELECT order_id, quantity, product_id, ROUND(unit_price, 2) AS unit_price_rounded FROM order_items WHERE product_id IS NOT NULL AND quantity BETWEEN 4 AND 5;
