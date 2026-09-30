SELECT
    id,
    amount,
    is_flagged,
    txn_time,
    ROUND(amount, 1) AS amount_rounded,
    DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time
FROM transactions;
