SELECT a.id, a.opened_date, a.balance, c.is_active, c.city, c.signup_date FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id ORDER BY id DESC LIMIT 10
