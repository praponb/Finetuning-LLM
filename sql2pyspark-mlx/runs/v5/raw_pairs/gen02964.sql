WITH with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
flagged AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.txn_time,
    s.merchant,
    s.prev_txn_time,
    s.is_new_burst,
    s.session_no,
    CASE WHEN txn_type RLIKE '^(CREDIT|DEBIT)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone,
    (SELECT MAX(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS max_transaction_amount
FROM islands s
