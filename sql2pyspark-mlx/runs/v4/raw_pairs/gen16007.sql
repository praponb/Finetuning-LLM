SELECT oi.id, oi.order_id, o.discount, s.region, ROUND(oi.quantity, 0) AS quantity_rounded FROM order_items oi INNER JOIN orders o ON o.id = oi.order_id INNER JOIN stores s ON o.store_id = s.id;
