SELECT status, MAX(balance) - MIN(balance) AS balance_range, COUNT(DISTINCT balance) AS unique_statuss FROM accounts GROUP BY 1 ORDER BY balance_range
