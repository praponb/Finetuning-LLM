WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount) AS rn
        FROM transactions
    ) x
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER w_recent AS running_sum_amount,
        AVG(amount) OVER w_recent AS running_avg_amount,
        MAX(amount) OVER w_recent AS running_max_amount
    FROM deduped
    WINDOW w_recent AS (PARTITION BY merchant ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_avg_amount, 1) OVER (PARTITION BY txn_type, merchant ORDER BY rn, id) AS prev_running_avg_amount,
        SUM(running_sum_amount) OVER (PARTITION BY merchant ORDER BY running_sum_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_amount,
        MAX(running_avg_amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS running_avg_amount_max_30d,
        MAX(running_avg_amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS running_avg_amount_max_7d
    FROM with_windows
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        b.balance,
        b.status,
        b.opened_date
    FROM sessions_transactions e
    INNER JOIN accounts b
        ON e.account_id = b.id
    WHERE b.balance IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(running_avg_amount) AS running_avg_amount_whole,
        running_max_amount * session_start_flag AS running_max_amount_times_session_start_flag
    FROM enriched
    WHERE txn_time >= DATE '2023-11-07'
      AND status = 'CLOSED'
)
-- Join a dimension and aggregate
SELECT
    d.status AS account_status,
    d.account_type,
    COUNT(*) AS num_rows,
    ROUND(AVG(a.session_no), 2) AS avg_session_no
FROM cleaned a
INNER JOIN accounts d ON a.account_id = d.id
GROUP BY d.status, d.account_type
ORDER BY account_status;
