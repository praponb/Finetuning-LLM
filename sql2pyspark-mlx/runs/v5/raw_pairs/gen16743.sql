WITH flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
islands AS (
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    CASE WHEN amount >= 4000 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    COUNT_IF(merchant = 'Shell') AS shell_events,
    MAX(txn_time) AS last_txn_time,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = 'Grab' THEN 1 END), 0), 2) AS amount_per_grab,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM islands
GROUP BY CASE WHEN amount >= 4000 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY amount_per_grab NULLS FIRST;
