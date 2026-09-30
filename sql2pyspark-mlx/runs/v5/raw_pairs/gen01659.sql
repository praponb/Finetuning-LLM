SELECT a.id, a.customer_id, a.balance, a.opened_date FROM accounts a WHERE a.customer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'CREDIT');
