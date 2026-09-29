SELECT id, status, opened_date, DENSE_RANK() OVER (PARTITION BY status ORDER BY opened_date DESC) AS opened_date_dense_rank FROM accounts ORDER BY opened_date ASC
