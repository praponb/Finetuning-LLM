SELECT t.id, t.amount, t.txn_time, t.txn_type FROM transactions t WHERE t.amount <= 3800 AND t.account_id IN (SELECT id FROM accounts WHERE balance < 38000) ORDER BY amount;
