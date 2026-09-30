WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        AVG(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3
    FROM accounts
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(status) AS status_lower,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM with_windows
    WHERE customer_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY running_balance DESC NULLS LAST) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance, 2) OVER w_recent AS prev_balance,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM latest
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY running_balance, id), w_ordered AS (PARTITION BY status ORDER BY balance_avg_3 DESC, id)
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(seq_no, 1) OVER (PARTITION BY account_type, status ORDER BY opened_date, id) AS prev_seq_no,
        LAST_VALUE(running_balance, TRUE) OVER w_recent AS last_known_running_balance,
        NTILE(5) OVER (ORDER BY balance, id) AS balance_bucket,
        LEAD(running_balance, 1, 0) OVER w_running AS next_running_balance
    FROM scored
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY prev_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY customer_id, status ORDER BY balance_avg_3, id)
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_amount
    FROM with_windows_accounts s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    SIZE(COLLECT_SET(status)) AS num_status,
    SUM(CASE WHEN account_type = 'LOAN' THEN next_running_balance ELSE 0 END) AS loan_next_running_balance,
    MAX(prev_balance) AS max_prev_balance,
    ROUND(AVG(last_known_running_balance), 2) AS avg_last_known_running_balance,
    APPROX_PERCENTILE(running_balance, ARRAY(0.25, 0.5, 0.75)) AS running_balance_quartiles
FROM with_last
GROUP BY week_start
HAVING max_prev_balance < 0;
