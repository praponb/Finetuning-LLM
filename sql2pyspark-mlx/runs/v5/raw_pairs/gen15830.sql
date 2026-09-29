SELECT status, MAX(balance) AS max_balance, SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_count FROM accounts WHERE status NOT IN ('FROZEN', 'OPEN') GROUP BY status;
