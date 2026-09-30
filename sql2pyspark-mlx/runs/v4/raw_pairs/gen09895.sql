SELECT
    id,
    merchant,
    amount,
    txn_type,
    FIRST_VALUE(txn_type) OVER (PARTITION BY merchant ORDER BY amount DESC, id) first_txn_type
FROM transactions
WHERE amount < 1700
