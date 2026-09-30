SELECT id, status, opened_date, RANK() OVER (PARTITION BY status ORDER BY opened_date) AS opened_date_rank FROM accounts
