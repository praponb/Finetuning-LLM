WITH ranked AS (SELECT id, status, balance, account_type, DENSE_RANK() OVER (PARTITION BY status ORDER BY balance) AS row_num FROM accounts) SELECT * FROM ranked WHERE row_num <= 3 ORDER BY status;
