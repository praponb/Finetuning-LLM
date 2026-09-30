SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    LEAD(amount, 2) OVER (PARTITION BY merchant ORDER BY amount, id) AS next_amount,
    LAG(amount, 1) OVER (PARTITION BY txn_type, merchant ORDER BY amount, id) AS prev_amount,
    FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY account_id, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant
FROM transactions
