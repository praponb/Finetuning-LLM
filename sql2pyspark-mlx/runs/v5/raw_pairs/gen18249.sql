SELECT id, txn_time, account_id, DENSE_RANK() OVER (ORDER BY txn_time) txn_time_dense_rank, COUNT(*) OVER (PARTITION BY account_id) account_id_count FROM transactions;
