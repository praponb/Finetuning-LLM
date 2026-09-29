SELECT o.id, o.status, o.discount, o.customer_id FROM orders o WHERE o.discount = (SELECT MIN(o2.discount) FROM orders o2 WHERE o2.status = o.status);
