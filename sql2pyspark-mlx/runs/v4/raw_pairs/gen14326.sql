SELECT a.id, a.customer_id, c.credit_limit, c.country FROM accounts a INNER JOIN customers c ON a.customer_id = c.id ORDER BY c.country ASC, id ASC LIMIT 5;
