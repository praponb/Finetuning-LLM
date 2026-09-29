WITH with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.signup_date,
        x.account_balance
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.balance AS account_balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        signup_date,
        account_balance,
        LAG(account_balance, 1) OVER (ORDER BY account_balance, id) AS prev_account_balance,
        PERCENT_RANK() OVER (ORDER BY account_balance NULLS FIRST) AS account_balance_percent_rank,
        ROW_NUMBER() OVER (ORDER BY account_balance DESC, id) AS event_seq
    FROM with_last
    WHERE signup_date >= '2023-06-30'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(prev_account_balance) AS prev_account_balance_whole
    FROM scored
    WHERE signup_date > TO_DATE('2023-04-12')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_account_balance_whole) OVER w_grp AS running_avg_prev_account_balance_whole,
        COUNT(prev_account_balance_whole) OVER w_grp AS running_count_prev_account_balance_whole,
        MIN(prev_account_balance_whole) OVER w_grp AS running_min_prev_account_balance_whole,
        MAX(prev_account_balance_whole) OVER w_grp AS running_max_prev_account_balance_whole
    FROM valid
    WINDOW w_grp AS (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.signup_date,
        s.account_balance,
        s.prev_account_balance,
        s.event_seq,
        s.prev_account_balance_whole,
        s.running_avg_prev_account_balance_whole,
        s.running_count_prev_account_balance_whole,
        s.running_min_prev_account_balance_whole,
        top1.account_status
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.session_browser
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid_customers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        signup_date,
        account_balance,
        prev_account_balance,
        event_seq,
        running_count_prev_account_balance_whole,
        running_min_prev_account_balance_whole,
        account_status,
        session_browser,
        CONCAT(session_browser, ' ', account_status) AS session_browser_account_status,
        SUBSTRING(session_browser, 1, 2) AS session_browser_prefix
    FROM latest_match
    WHERE signup_date >= DATE '2023-03-12'
      AND account_status IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    CUME_DIST() OVER w_ordered AS prev_account_balance_cume_dist,
    NTILE(3) OVER w_grp AS running_min_prev_account_balance_whole_bucket,
    NTILE(4) OVER (PARTITION BY session_browser ORDER BY running_count_prev_account_balance_whole, id) AS running_count_prev_account_balance_whole_bucket
FROM valid_customers
WINDOW w_ordered AS (PARTITION BY account_status ORDER BY prev_account_balance DESC NULLS FIRST), w_grp AS (PARTITION BY account_status ORDER BY running_min_prev_account_balance_whole DESC, id)
