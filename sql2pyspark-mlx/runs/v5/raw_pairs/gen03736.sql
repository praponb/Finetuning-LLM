SELECT a.id, a.balance FROM accounts a WHERE a.customer_id = 33 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY a.id DESC, a.balance DESC LIMIT 50;
