SELECT id, txn_type, txn_time FROM (SELECT id, txn_type, txn_time, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) pos FROM transactions) ranked WHERE pos = 1
