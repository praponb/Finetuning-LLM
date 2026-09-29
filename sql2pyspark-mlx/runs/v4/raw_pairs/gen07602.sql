WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS row_num FROM transactions) SELECT id, txn_type, amount FROM ranked WHERE row_num <= 5 ORDER BY txn_type ASC
