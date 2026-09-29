SELECT id, status, opened_date, balance, LAG(balance, 1) OVER (PARTITION BY status ORDER BY opened_date, id) AS prev_balance FROM accounts ORDER BY opened_date ASC, status ASC;
