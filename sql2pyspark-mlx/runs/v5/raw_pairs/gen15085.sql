WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        LEAD(amount, 2) OVER (PARTITION BY merchant ORDER BY amount, id) AS next_amount,
        MAX(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d
    FROM transactions
)
SELECT
    id,
    amount,
    is_flagged,
    last_known_merchant,
    next_amount
FROM with_windows
ORDER BY next_amount DESC
