SELECT MAX(quantity) - MIN(quantity) AS quantity_range, ROUND(AVG(unit_price), 2) AS avg_unit_price FROM order_items WHERE product_id IN (2, 5, 40) ORDER BY quantity_range;
