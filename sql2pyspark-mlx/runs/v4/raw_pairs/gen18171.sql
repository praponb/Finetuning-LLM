WITH with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
)
SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM with_gaps
