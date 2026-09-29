SELECT quantity, ROUND(unit_price * 0.9, 2) AS adjusted_unit_price FROM order_items WHERE quantity BETWEEN 3 AND 8 ORDER BY quantity DESC, adjusted_unit_price DESC;
