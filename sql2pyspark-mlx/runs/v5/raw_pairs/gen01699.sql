SELECT a.id, a.customer_id
FROM accounts a
WHERE a.account_type = 'CHECKING' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type IN ('FEE', 'CREDIT', 'REFUND'))
