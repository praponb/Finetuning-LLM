SELECT a.opened_date, a.status, a.customer_id, c.email FROM accounts a JOIN customers c ON a.customer_id = c.id ORDER BY status, email DESC
