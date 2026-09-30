WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.prev_txn_time,
    s.is_new_burst,
    CASE WHEN txn_type RLIKE '^(FEE|REFUND)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS txn_type_zone,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM sessions_transactions s
ORDER BY txn_type_zone NULLS FIRST, txn_time;
