SELECT status, account_type, COUNT(*) AS num_accounts FROM accounts GROUP BY status, account_type
