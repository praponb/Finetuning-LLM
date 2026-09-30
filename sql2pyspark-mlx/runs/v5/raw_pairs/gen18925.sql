SELECT o.status, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price FROM order_items oi JOIN orders o ON o.id = oi.order_id GROUP BY o.status
