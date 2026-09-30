SELECT id, account_id
FROM transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY txn_time ASC NULLS FIRST, id ASC) = 1
