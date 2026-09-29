SELECT oi.id, oi.product_id, o.channel FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id ORDER BY channel DESC;
