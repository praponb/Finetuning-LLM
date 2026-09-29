WITH gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessions_transactions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    is_new_burst,
    burst_id,
    CASE WHEN merchant RLIKE '^(Lazada|Shell)$' THEN 'OTHER_ZONE' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS merchant_zone
FROM sessions_transactions
ORDER BY txn_time ASC;
