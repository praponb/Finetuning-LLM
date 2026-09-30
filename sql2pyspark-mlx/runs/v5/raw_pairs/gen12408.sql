WITH flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessionized AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    txn_time,
    is_flagged
FROM sessionized
