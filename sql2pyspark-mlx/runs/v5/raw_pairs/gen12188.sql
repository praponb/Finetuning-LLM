SELECT
    id,
    txn_type,
    merchant,
    txn_time,
    account_id,
    FIRST_VALUE(account_id) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id) first_account_id
FROM transactions
