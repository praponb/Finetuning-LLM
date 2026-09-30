WITH cleaned AS (
    SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 10 AND 15
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    merchant,
    MAX(txn_time) AS last_seen,
    MAX(txn_time) AS ended_at,
    APPROX_PERCENTILE(amount, 0.9) AS amount_p90
FROM islands
GROUP BY merchant
HAVING amount_p90 >= 0
ORDER BY amount_p90 NULLS LAST;
