SELECT t.id, t.txn_type, t.txn_time, t.is_flagged FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE balance >= 36000);
