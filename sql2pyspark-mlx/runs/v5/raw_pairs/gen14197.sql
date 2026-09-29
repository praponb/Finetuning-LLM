WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.session_no,
    (SELECT MIN(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS min_transaction_amount
FROM sessionized s
