SELECT status FROM accounts WHERE balance BETWEEN 21000 AND 78000 UNION SELECT status FROM subscriptions WHERE customer_id IS NULL ORDER BY status DESC
