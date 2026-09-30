SELECT id, txn_time, ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) AS txn_time_row FROM transactions;
