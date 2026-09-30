SELECT oi.id, oi.quantity, o.discount, p.price FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id RIGHT OUTER JOIN products p ON oi.product_id = p.id
