SELECT a.id, a.status, a.account_type, a.customer_id FROM accounts a WHERE a.customer_id IN (16, 37) AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
