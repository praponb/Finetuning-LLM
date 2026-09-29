WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    CASE WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'EXTENDED' WHEN txn_type RLIKE '^(DEBIT|FEE)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS txn_type_zone,
    (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS min_transaction_amount
FROM sessions_transactions s;
