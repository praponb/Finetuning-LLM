WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(island_id, 0.9) AS island_id_p90,
    MIN(is_new_session) AS min_is_new_session,
    APPROX_PERCENTILE(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles
FROM sessionized
GROUP BY DATE_TRUNC('WEEK', txn_time)
HAVING APPROX_PERCENTILE(island_id, 0.9) > 0
ORDER BY week_start DESC NULLS LAST;
