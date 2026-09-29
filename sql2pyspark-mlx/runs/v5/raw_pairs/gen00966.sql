SELECT account_type, SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_count FROM accounts GROUP BY 1;
