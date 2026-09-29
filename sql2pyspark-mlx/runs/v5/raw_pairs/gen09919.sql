SELECT a.balance, a.opened_date, a.status, c.signup_date, c.country, c.city FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.email LIKE '%@company.co';
