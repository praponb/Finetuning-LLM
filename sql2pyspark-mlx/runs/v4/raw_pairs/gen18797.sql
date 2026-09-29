SELECT a.id, a.balance, a.status, a.customer_id, c.signup_date, c.credit_limit FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id WHERE c.name LIKE '%Dia%';
