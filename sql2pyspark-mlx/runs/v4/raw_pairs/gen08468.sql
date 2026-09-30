SELECT id, status, account_type, ROUND(balance * 0.85, 2) AS adjusted_balance FROM accounts WHERE NOT (status = 'OPEN' AND customer_id = 29 AND status IN ('CLOSED', 'FROZEN'));
