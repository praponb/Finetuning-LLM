SELECT a.customer_id, a.opened_date, a.balance, c.is_active FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id;
