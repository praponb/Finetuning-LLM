WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_ADD(opened_date, 65) AS opened_date_plus_65d,
        CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END AS is_checking
    FROM accounts
    WHERE status IS NOT NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.txn_time
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY customer_id ORDER BY txn_time ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_last
),
sessions_accounts AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY is_checking ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS balance_avg_2,
        RANK() OVER (PARTITION BY status, account_type ORDER BY is_checking ASC NULLS LAST) AS is_checking_rank,
        SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_burst,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY opened_date_plus_65d ASC, id ASC) AS row_seq,
        MAX(is_checking) OVER (PARTITION BY status) AS status_max_is_checking
    FROM sessions_accounts
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_checking >= 63 THEN 'LARGE' ELSE 'SMALL' END AS is_checking_tier,
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    MAX(balance_avg_2) AS max_balance_avg_2,
    COUNT_IF(account_type = 'CREDIT') AS credit_events
FROM metrics
GROUP BY CASE WHEN is_checking >= 63 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', txn_time)
ORDER BY avg_row_seq DESC;
