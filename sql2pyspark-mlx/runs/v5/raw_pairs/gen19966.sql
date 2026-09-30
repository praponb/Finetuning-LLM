SELECT id, customer_id, status, opened_date, DENSE_RANK() OVER (PARTITION BY customer_id, status ORDER BY opened_date DESC) AS opened_date_dense_rank FROM accounts
