SELECT
    id,
    txn_time,
    amount,
    txn_type,
    merchant,
    SUM(amount) OVER (ORDER BY txn_time, id) AS running_amount,
    FIRST_VALUE(merchant) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS first_merchant
FROM transactions
WHERE is_flagged = TRUE;
