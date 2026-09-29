SELECT t.id, t.amount, t.txn_type, t.account_id FROM transactions t WHERE t.account_id IN (5, 22, 28) AND t.account_id IN (SELECT id FROM accounts WHERE status = 'FROZEN')
