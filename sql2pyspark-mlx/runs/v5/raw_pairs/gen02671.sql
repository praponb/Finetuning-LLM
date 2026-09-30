SELECT a.id, a.balance, a.status, a.customer_id FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'FEE') ORDER BY balance DESC;
