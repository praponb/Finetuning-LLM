SELECT a.id, a.customer_id, a.status FROM accounts a WHERE a.customer_id = 37 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
