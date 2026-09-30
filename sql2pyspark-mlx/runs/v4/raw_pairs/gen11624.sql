SELECT shipments.shipped_date, shipments.carrier, orders.discount, orders.status, orders.amount FROM shipments RIGHT OUTER JOIN orders ON shipments.order_id = orders.id ORDER BY discount DESC;
