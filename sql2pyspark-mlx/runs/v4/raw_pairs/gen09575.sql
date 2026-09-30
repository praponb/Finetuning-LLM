SELECT oi.unit_price, o.customer_id, p.product_name
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
FULL OUTER JOIN products p ON p.id = oi.product_id
ORDER BY product_name DESC
