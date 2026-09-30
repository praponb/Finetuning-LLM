WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(merchant, 1, 2) AS merchant_prefix,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE is_flagged
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, account_id
    FROM islands
    EXCEPT DISTINCT
    SELECT id, account_id
    FROM transactions
    WHERE is_flagged
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN matched_keys k ON s.id = k.id AND s.account_id = k.account_id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        account_id,
        DATE_TRUNC('WEEK', txn_time) AS week_start,
        SUM(amount) AS total_amount,
        PERCENTILE_APPROX(session_no, 0.5) AS session_no_p50,
        RANK() OVER (PARTITION BY account_id ORDER BY SUM(amount) DESC) AS total_amount_rank
    FROM verified
    GROUP BY 1, 2
)
SELECT
    account_id,
    week_start,
    total_amount,
    session_no_p50,
    total_amount_rank
FROM grouped
ORDER BY week_start DESC NULLS FIRST, session_no_p50 DESC
