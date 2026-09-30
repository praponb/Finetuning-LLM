SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    FIRST_VALUE(amount, TRUE) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS amount_percent_rank
FROM transactions
