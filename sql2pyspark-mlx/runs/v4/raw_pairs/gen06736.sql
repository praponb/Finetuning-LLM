WITH top_transactions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) row_num FROM transactions) SELECT * FROM top_transactions WHERE row_num <= 1;
