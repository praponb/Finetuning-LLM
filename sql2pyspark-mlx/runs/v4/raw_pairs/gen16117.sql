SELECT a.status, a.customer_id, c.credit_limit, c.is_active, c.city FROM accounts a INNER JOIN customers c ON a.customer_id = c.id ORDER BY c.credit_limit, status;
