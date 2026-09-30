SELECT status, 'account' AS source FROM accounts WHERE account_type = 'SAVINGS' UNION SELECT status, 'subscription' AS source FROM subscriptions WHERE customer_id = 31 ORDER BY source DESC;
