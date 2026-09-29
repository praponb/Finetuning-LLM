SELECT t.id, t.txn_type FROM transactions t WHERE t.amount >= 2800 AND t.account_id NOT IN (SELECT id FROM accounts WHERE status IN ('OPEN', 'CLOSED'))
