WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) AS row_num FROM accounts) SELECT id, status, opened_date, customer_id FROM ranked WHERE row_num <= 3;
