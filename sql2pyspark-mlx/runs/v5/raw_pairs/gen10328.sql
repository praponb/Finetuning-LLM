SELECT c.segment, MAX(a.balance) AS max_balance, MAX(a.balance) - MIN(a.balance) AS balance_range FROM accounts a JOIN customers c ON a.customer_id = c.id GROUP BY c.segment;
