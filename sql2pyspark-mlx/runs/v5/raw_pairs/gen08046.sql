SELECT *, ROUND(unit_price * 1.1, 2) AS unit_price_adjusted FROM order_items WHERE product_id IS NOT NULL AND unit_price BETWEEN 170 AND 320 AND quantity < 8;
