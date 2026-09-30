SELECT o.id, o.channel, o.status, o.order_date, c.segment FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.store_id IS NOT NULL;
