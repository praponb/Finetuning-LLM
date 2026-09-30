SELECT a.id, a.customer_id, a.status, a.balance FROM accounts a WHERE a.customer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time >= '2024-05-17');
