SELECT t.id, t.amount, t.txn_time, t.txn_type FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE opened_date >= '2023-08-01');
