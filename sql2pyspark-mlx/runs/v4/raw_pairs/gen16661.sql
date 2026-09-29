SELECT o.id, o.amount, c.city, c.country FROM orders o RIGHT OUTER JOIN customers c ON o.customer_id = c.id WHERE o.status IN ('SHIPPED', 'RETURNED', 'CANCELLED');
