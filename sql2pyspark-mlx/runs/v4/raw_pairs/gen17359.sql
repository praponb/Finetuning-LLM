SELECT a.id, a.customer_id, a.opened_date, a.account_type, c.country, c.signup_date FROM accounts a RIGHT JOIN customers c ON a.customer_id = c.id;
