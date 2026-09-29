SELECT a.id, a.customer_id, a.status
FROM accounts a
WHERE a.balance < 47000 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'REFUND');
