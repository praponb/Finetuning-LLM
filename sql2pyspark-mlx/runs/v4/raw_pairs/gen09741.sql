SELECT a.id, a.opened_date, c.credit_limit FROM accounts a INNER JOIN customers c ON c.id = a.customer_id ORDER BY a.opened_date DESC, id;
