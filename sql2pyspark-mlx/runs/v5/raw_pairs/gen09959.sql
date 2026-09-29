SELECT o.id, o.order_date, o.status FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE LENGTH(name) < 12)
