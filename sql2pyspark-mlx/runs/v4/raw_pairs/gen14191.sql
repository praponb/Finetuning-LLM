SELECT c.is_active, MAX(a.balance) - MIN(a.balance) balance_range FROM accounts a FULL OUTER JOIN customers c ON a.customer_id = c.id GROUP BY c.is_active;
