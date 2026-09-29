SELECT t.id, t.txn_time, t.account_id, t.amount FROM transactions t WHERE t.amount <= 1300 AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance < 60000) ORDER BY account_id DESC;
