SELECT a.id, a.status, a.balance, a.opened_date, c.email, c.city, c.is_active FROM accounts a LEFT OUTER JOIN customers c ON a.customer_id = c.id WHERE c.is_active AND a.opened_date <= '2023-10-01';
