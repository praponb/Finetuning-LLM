SELECT
    id,
    txn_type,
    txn_time,
    merchant,
    LEAD(amount, 1, 0) OVER (ORDER BY amount ASC, id ASC) AS next_amount,
    ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id ASC) AS event_seq,
    LAST_VALUE(amount, TRUE) OVER w_recent AS last_known_amount
FROM transactions
WHERE account_id IN (9, 14, 26, 30)
WINDOW w_recent AS (PARTITION BY txn_type ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
