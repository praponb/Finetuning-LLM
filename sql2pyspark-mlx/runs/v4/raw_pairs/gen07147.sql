WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    account_id,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN amount >= 3100 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    PERCENTILE_APPROX(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles,
    MIN(session_id) AS min_session_id,
    MIN(txn_time) AS first_seen,
    COUNT(*) FILTER (WHERE txn_type = 'REFUND') AS refund_rows,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN amount_int ELSE 0 END) AS credit_amount_int,
    COUNT_IF(merchant = 'Shell') AS shell_events
FROM sessionized
GROUP BY account_id, DATE_TRUNC('WEEK', txn_time), CASE WHEN amount >= 3100 THEN 'LARGE' ELSE 'SMALL' END
HAVING shell_events > 1;
