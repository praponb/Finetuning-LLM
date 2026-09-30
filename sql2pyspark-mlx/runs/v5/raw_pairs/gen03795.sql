SELECT id, merchant, txn_time, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS txn_time_row FROM transactions;
