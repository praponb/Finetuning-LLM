SELECT o.id, o.shipping_fee, o.customer_id, o.order_date FROM orders o WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE LENGTH(email) >= 7) ORDER BY order_date;
