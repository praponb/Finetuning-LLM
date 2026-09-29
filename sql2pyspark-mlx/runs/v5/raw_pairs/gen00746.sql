SELECT o.id, o.customer_id, o.status, c.is_active, c.credit_limit FROM orders o FULL OUTER JOIN customers c ON c.id = o.customer_id ORDER BY id DESC
