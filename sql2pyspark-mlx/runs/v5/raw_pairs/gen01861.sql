SELECT a.id, a.balance, a.status, a.customer_id, c.segment FROM accounts a LEFT OUTER JOIN customers c ON a.customer_id = c.id ORDER BY c.segment
