SELECT t.id, t.amount, a.status, a.balance, c.email FROM transactions t RIGHT OUTER JOIN accounts a ON a.id = t.account_id INNER JOIN customers c ON c.id = a.customer_id;
