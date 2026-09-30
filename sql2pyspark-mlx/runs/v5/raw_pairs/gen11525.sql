SELECT status, account_type, MIN(balance) min_balance
FROM accounts
WHERE customer_id IN (8, 12, 13)
GROUP BY status, account_type
