WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        LAG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date DESC, id) AS row_seq_2
    FROM accounts
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        COUNT(prev_balance) OVER (PARTITION BY account_type) AS account_type_count_prev_balance,
        AVG(prev_balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS prev_balance_avg_5
    FROM metrics
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        balance,
        prev_balance,
        row_seq,
        row_seq_2,
        prev_balance_avg_5,
        SUM(account_type_count_prev_balance) OVER (ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_account_type_count_prev_balance,
        CUME_DIST() OVER (ORDER BY row_seq_2 NULLS LAST) AS row_seq_2_cume_dist
    FROM scored
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        balance,
        row_seq,
        row_seq_2,
        prev_balance_avg_5,
        running_account_type_count_prev_balance,
        row_seq_2_cume_dist,
        ABS(balance) AS abs_balance,
        LEAST(row_seq_2_cume_dist, running_account_type_count_prev_balance) AS least_row_seq_2_cume_dist_running_account_type_count_prev_balance
    FROM with_windows
    WHERE prev_balance_avg_5 BETWEEN 49127 AND 76119
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        balance,
        row_seq_2,
        running_account_type_count_prev_balance,
        abs_balance,
        ROUND(least_row_seq_2_cume_dist_running_account_type_count_prev_balance, 2) AS least_row_seq_2_cume_dist_running_account_type_count_prev_balance_rounded
    FROM filtered
    WHERE abs_balance <= 38
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.amount
    FROM valid s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(balance, 1) OVER (ORDER BY balance, id) AS next_balance,
        NTILE(4) OVER w_running AS abs_balance_bucket,
        ROW_NUMBER() OVER w_hist AS row_seq,
        LEAD(amount, 1, 0) OVER (ORDER BY running_account_type_count_prev_balance, id) AS next_amount,
        LAST_VALUE(least_row_seq_2_cume_dist_running_account_type_count_prev_balance_rounded) IGNORE NULLS OVER (ORDER BY running_account_type_count_prev_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_row_seq_2_cume_dist_running_account_type_count_prev_balance_rounded
    FROM with_latest
    WINDOW w_running AS (ORDER BY abs_balance, id), w_hist AS (ORDER BY running_account_type_count_prev_balance DESC, id)
)
-- Grouped metrics
SELECT
    CASE WHEN abs_balance_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS abs_balance_bucket_tier,
    AVG(row_seq_2) AS avg_row_seq_2,
    PERCENTILE_APPROX(balance, 0.5) AS balance_p50,
    COUNT(*) AS row_count,
    ROUND(AVG(row_seq_2), 2) AS avg_row_seq_2_2,
    PERCENTILE_APPROX(abs_balance, ARRAY(0.25, 0.5, 0.75)) AS abs_balance_quartiles
FROM metrics_accounts
GROUP BY CASE WHEN abs_balance_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY row_count NULLS FIRST;
