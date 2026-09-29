SELECT AVG(quantity) AS avg_quantity, SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE order_id = 25 AND product_id IN (1, 12, 24, 35) AND quantity BETWEEN 4 AND 8
