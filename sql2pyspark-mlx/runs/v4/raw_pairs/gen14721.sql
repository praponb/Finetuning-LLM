SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    COUNT(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_count_90d,
    ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq
FROM transactions
