SELECT a.id, a.opened_date, a.customer_id FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY 3 DESC, id LIMIT 20
