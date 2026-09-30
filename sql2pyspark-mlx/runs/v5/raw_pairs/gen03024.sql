WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-04-13 19:00:00'
)
-- Running and moving aggregates
SELECT
    id,
    account_id,
    amount,
    merchant,
    ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share,
    LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type, merchant), 4) AS amount_share_2,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS event_seq
FROM cleaned
WHERE amount < 3800
