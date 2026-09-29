SELECT t.id, t.amount, t.txn_type, t.account_id FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE status IN ('CLOSED', 'FROZEN'));
