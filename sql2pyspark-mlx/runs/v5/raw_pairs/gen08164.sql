SELECT a.id, a.opened_date
FROM accounts a
WHERE a.opened_date <= '2025-04-05' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type IN ('FEE', 'REFUND'))
ORDER BY a.id;
