WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    SUM(CASE WHEN txn_type = 'FEE' THEN session_no ELSE 0 END) AS fee_session_no,
    SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount,
    STDDEV_SAMP(amount) AS amount_stddev
FROM sessions_transactions
GROUP BY ROLLUP (DATE_TRUNC('YEAR', txn_time), DATE_TRUNC('WEEK', txn_time), CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY amount_stddev DESC NULLS FIRST;
