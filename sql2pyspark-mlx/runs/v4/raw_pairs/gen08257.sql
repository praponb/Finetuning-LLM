SELECT a.id, a.customer_id, a.opened_date FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY customer_id, a.id;
