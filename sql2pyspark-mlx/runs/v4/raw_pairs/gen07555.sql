SELECT a.id, a.balance, a.opened_date, a.customer_id, c.city FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.is_active AND YEAR(a.opened_date) = 2023;
