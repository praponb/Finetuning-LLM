SELECT status, MIN(opened_date) AS min_opened_date, COUNT(DISTINCT balance) AS unique_account_types FROM accounts WHERE status IS NULL AND balance >= 36000 GROUP BY status;
