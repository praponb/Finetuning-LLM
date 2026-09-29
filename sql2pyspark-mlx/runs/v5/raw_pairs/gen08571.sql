SELECT a.id, a.status, a.opened_date, c.city, c.country FROM accounts a INNER JOIN customers c ON a.customer_id = c.id;
