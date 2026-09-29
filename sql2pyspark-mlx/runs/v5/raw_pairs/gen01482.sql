WITH flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
)
SELECT
    id,
    account_id,
    txn_time,
    merchant,
    is_flagged,
    SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM flagged
