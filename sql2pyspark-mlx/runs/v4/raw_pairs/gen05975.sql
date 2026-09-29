SELECT a.id, a.customer_id
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time < '2025-05-02')
ORDER BY a.id
