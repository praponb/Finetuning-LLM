WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        end_date,
        monthly_fee,
        status,
        SUM(monthly_fee) OVER (ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM subscriptions
    WINDOW w_hist AS (PARTITION BY status ORDER BY start_date DESC, id ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        running_monthly_fee,
        row_seq,
        COUNT(row_seq) OVER w_part AS running_count_row_seq,
        MIN(row_seq) OVER w_part AS running_min_row_seq,
        MAX(row_seq) OVER w_part AS running_max_row_seq,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY running_monthly_fee ASC) AS running_monthly_fee_percent_rank
    FROM scored
    WINDOW w_part AS (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_max_row_seq) OVER (PARTITION BY status ORDER BY running_max_row_seq ASC, id ASC) AS prev_running_max_row_seq,
        LEAD(running_max_row_seq) OVER (PARTITION BY status ORDER BY running_max_row_seq ASC, id ASC) AS next_running_max_row_seq,
        LEAD(running_monthly_fee) OVER (PARTITION BY status ORDER BY running_max_row_seq ASC, id ASC) AS next_running_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_max_row_seq ASC, id ASC) AS row_seq_2
    FROM with_windows
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows_subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY next_running_max_row_seq DESC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        running_monthly_fee,
        row_seq,
        running_count_row_seq,
        running_min_row_seq,
        running_max_row_seq,
        running_monthly_fee_percent_rank,
        prev_running_max_row_seq,
        next_running_max_row_seq,
        row_seq_2,
        LAST_VALUE(next_running_monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY next_running_monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_running_monthly_fee,
        MIN(row_seq) OVER (PARTITION BY status) AS status_min_row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY row_seq DESC, id ASC) AS row_seq_3
    FROM latest
    WHERE running_monthly_fee_percent_rank BETWEEN 0.44 AND 0.51
)
-- Window metrics over several different partitions
SELECT
    id,
    running_count_row_seq,
    running_monthly_fee_percent_rank,
    row_seq_2,
    NTILE(3) OVER (ORDER BY running_count_row_seq DESC, id ASC) AS running_count_row_seq_bucket,
    AVG(row_seq_3) OVER (PARTITION BY status ORDER BY running_min_row_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_seq_3_avg_6,
    RANK() OVER (PARTITION BY status ORDER BY row_seq_2 DESC NULLS LAST) AS row_seq_2_rank
FROM metrics;
