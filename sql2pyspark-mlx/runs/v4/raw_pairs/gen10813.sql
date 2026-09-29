SELECT a.status, a.opened_date, a.customer_id, c.email, c.city, c.signup_date FROM accounts a INNER JOIN customers c ON c.id = a.customer_id;
