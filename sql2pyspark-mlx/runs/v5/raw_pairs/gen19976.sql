SELECT c.segment, COUNT(*) AS num_accounts, COUNT(CASE WHEN a.account_type = 'CREDIT' THEN 1 END) AS credit_count FROM accounts a JOIN customers c ON c.id = a.customer_id GROUP BY c.segment;
