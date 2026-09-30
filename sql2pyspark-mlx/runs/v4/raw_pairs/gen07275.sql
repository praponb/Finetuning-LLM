SELECT a.id, a.status, a.customer_id, a.balance FROM accounts a WHERE a.customer_id IS NOT NULL AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
