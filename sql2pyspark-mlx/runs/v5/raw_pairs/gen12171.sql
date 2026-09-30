SELECT a.id, a.status
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND TO_DATE(t.txn_time) < '2024-07-01')
