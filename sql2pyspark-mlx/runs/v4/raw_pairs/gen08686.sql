SELECT id, status, opened_date, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS opened_date_row FROM accounts
