SELECT SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END) AS loan_count
FROM accounts
