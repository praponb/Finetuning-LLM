SELECT a.id, a.account_type, a.status, a.balance, c.segment FROM accounts a LEFT OUTER JOIN customers c ON c.id = a.customer_id ORDER BY c.segment;
