SELECT id, status, opened_date, balance FROM (SELECT id, status, opened_date, balance, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS rn FROM accounts) AS r WHERE rn <= 3
