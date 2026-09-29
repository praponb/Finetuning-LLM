SELECT AVG(quantity) AS avg_quantity, SUM(unit_price * quantity) AS revenue
FROM order_items
WHERE order_id IN (12, 19, 34);
