SELECT t.id, t.txn_type, t.amount
FROM transactions t
WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NOT NULL)
