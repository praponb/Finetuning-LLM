SELECT t.txn_type, a.balance, c.city
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
WHERE a.customer_id = 20;
