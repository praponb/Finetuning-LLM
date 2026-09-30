WITH gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
sessions_transactions AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.new_group_flag,
    s.session_no,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM sessions_transactions s
ORDER BY txn_time DESC NULLS FIRST
