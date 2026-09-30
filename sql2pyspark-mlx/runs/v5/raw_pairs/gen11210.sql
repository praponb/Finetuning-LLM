SELECT product_id, order_id, SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE order_id IN (13, 18, 23, 29)
GROUP BY product_id, order_id
