SELECT CASE WHEN balance >= 85000 THEN 'high' WHEN balance >= 15000 THEN 'mid' ELSE 'low' END AS balance_bucket, account_type, status, MAX(balance) AS max_balance FROM accounts GROUP BY 1, 2, 3
