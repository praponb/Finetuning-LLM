SELECT a.id, a.account_type, a.opened_date FROM accounts a WHERE a.account_type = 'SAVINGS' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND HOUR(t.txn_time) BETWEEN 7 AND 15)
