SELECT a.status, a.opened_date, a.account_type, c.age, c.city, c.credit_limit FROM accounts a INNER JOIN customers c ON a.customer_id = c.id ORDER BY credit_limit DESC;
