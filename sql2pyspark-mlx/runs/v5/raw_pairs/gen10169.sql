WITH valid AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), txn_time) days_since_txn_time,
        DATEDIFF(CURRENT_DATE(), txn_time) days_since_txn_time_2
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time < '2025-05-01'
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        days_since_txn_time_2,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    SELECT
        *,
        CAST(txn_time AS DATE) txn_time_day,
        days_since_txn_time_2 - session_no days_since_txn_time_2_minus_session_no
    FROM islands
    WHERE merchant IS NOT NULL
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY session_no) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
)
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    AVG(session_no) FILTER (WHERE merchant = 'Shell') AS shell_avg_session_no,
    MIN(txn_time) AS started_at,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    MIN(txn_time) AS started_at_2,
    APPROX_PERCENTILE(days_since_txn_time_2_minus_session_no, ARRAY(0.25, 0.5, 0.75)) AS days_since_txn_time_2_minus_session_no_quartiles
FROM deduped
GROUP BY DATE_TRUNC('MONTH', txn_time);
