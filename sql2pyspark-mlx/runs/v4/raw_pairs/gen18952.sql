SELECT a.id, a.customer_id, a.status
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (21, 23, 33, 38))
