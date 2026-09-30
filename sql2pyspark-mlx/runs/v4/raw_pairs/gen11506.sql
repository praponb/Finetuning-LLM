SELECT a.id, a.customer_id
FROM accounts a
WHERE a.status IN ('CLOSED', 'FROZEN', 'OPEN') AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id = 19);
