SELECT status, SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END) AS loan_count, COUNT(DISTINCT balance) AS unique_account_types FROM accounts GROUP BY status;
