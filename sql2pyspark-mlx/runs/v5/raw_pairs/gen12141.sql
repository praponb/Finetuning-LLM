SELECT id, account_type, LOWER(status) AS status_lower, ROUND(balance * 0.85, 2) AS adjusted_balance FROM accounts WHERE customer_id = 32 ORDER BY status_lower DESC;
