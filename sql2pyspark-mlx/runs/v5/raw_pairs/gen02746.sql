SELECT id, txn_type, txn_time, RANK() OVER (PARTITION BY txn_type ORDER BY txn_time) AS rnk FROM transactions;
