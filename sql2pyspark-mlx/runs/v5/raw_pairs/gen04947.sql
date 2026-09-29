SELECT o.order_date, s.status, s.carrier FROM orders o JOIN shipments s ON s.order_id = o.id
