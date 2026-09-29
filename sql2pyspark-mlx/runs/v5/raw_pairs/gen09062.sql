SELECT o.status, AVG(oi.unit_price) avg_unit_price, COUNT(oi.unit_price) unit_price_count FROM order_items oi FULL OUTER JOIN orders o ON oi.order_id = o.id GROUP BY o.status;
