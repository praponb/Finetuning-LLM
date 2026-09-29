WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    txn_type,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    SUM(is_new_session) FILTER (WHERE txn_type = 'FEE') AS fee_total_is_new_session,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    SUM(CASE WHEN merchant = '7-Eleven' THEN amount ELSE 0 END) AS 7_eleven_amount,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', txn_time) ORDER BY (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 DESC) AS duration_min_percent_rank
FROM sessionized
GROUP BY DATE_TRUNC('QUARTER', txn_time), txn_type
HAVING SUM(is_new_session) FILTER (WHERE txn_type = 'FEE') < 1000000
ORDER BY 7_eleven_amount ASC;
