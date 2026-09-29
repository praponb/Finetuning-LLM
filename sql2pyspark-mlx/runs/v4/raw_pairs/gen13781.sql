SELECT a.id, a.customer_id FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time < '2024-08-04');
