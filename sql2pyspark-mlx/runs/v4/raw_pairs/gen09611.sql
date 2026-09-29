SELECT order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
WHERE product_id = 39 AND (order_id IN (1, 14, 29, 36) OR quantity <> 4)
GROUP BY 1
HAVING MAX(quantity) < 3.0
