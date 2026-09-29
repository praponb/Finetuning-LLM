SELECT
    id,
    amount,
    txn_time,
    merchant,
    UPPER(txn_type) AS txn_type_upper
FROM transactions
ORDER BY txn_type_upper ASC;
