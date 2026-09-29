WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER w_part AS balance_bucket,
        SUM(balance) OVER (PARTITION BY status) AS status_sum_balance,
        ROW_NUMBER() OVER w_part AS row_seq,
        SUM(balance) OVER w_grp AS status_sum_balance_2,
        LEAD(balance, 2) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC) AS next_balance
    FROM accounts
    WINDOW w_part AS (PARTITION BY account_type ORDER BY balance DESC, id ASC), w_grp AS (PARTITION BY status, customer_id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_bucket,
        status_sum_balance,
        row_seq,
        status_sum_balance_2,
        next_balance,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY next_balance DESC, id ASC) AS next_balance_bucket,
        STDDEV_SAMP(balance_bucket) OVER (PARTITION BY account_type) AS account_type_stddev_samp_balance_bucket,
        AVG(balance_bucket) OVER (PARTITION BY status, account_type ORDER BY next_balance ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_bucket_avg_5_prior
    FROM metrics
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY status_sum_balance_2 DESC, id ASC) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY next_balance ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        AVG(next_balance) OVER (PARTITION BY customer_id ORDER BY balance_bucket_avg_5_prior ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_balance_avg_6
    FROM ranked
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_bucket,
        status_sum_balance,
        row_seq,
        status_sum_balance_2,
        next_balance,
        next_balance_bucket,
        account_type_stddev_samp_balance_bucket,
        balance_bucket_avg_5_prior,
        latest_rank,
        balance_avg_6_prior,
        next_balance_avg_6,
        SUM(status_sum_balance) OVER (PARTITION BY customer_id ORDER BY account_type_stddev_samp_balance_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_sum_balance,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance_bucket_avg_5_prior DESC NULLS FIRST) AS balance_bucket_avg_5_prior_percent_rank,
        AVG(status_sum_balance_2) OVER (PARTITION BY customer_id ORDER BY balance_bucket ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_sum_balance_2_avg_3_prior
    FROM with_windows
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM with_windows_accounts
    EXCEPT DISTINCT
    SELECT status, id
    FROM accounts
    WHERE account_type IN ('CREDIT', 'SAVINGS')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_accounts s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
enriched_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_bucket,
        status_sum_balance,
        row_seq,
        status_sum_balance_2,
        next_balance,
        next_balance_bucket,
        account_type_stddev_samp_balance_bucket,
        balance_bucket_avg_5_prior,
        latest_rank,
        balance_avg_6_prior,
        next_balance_avg_6,
        running_status_sum_balance,
        balance_bucket_avg_5_prior_percent_rank,
        status_sum_balance_2_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY status ORDER BY row_seq DESC NULLS FIRST) AS row_seq_cume_dist,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY status_sum_balance_2_avg_3_prior DESC NULLS FIRST) AS status_sum_balance_2_avg_3_prior_dense_rank
    FROM verified
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        MAX(next_balance_avg_6) FILTER (WHERE status = 'OPEN') AS open_max_next_balance_avg_6,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(DISTINCT account_type) AS distinct_account_types,
        ROUND(AVG(next_balance_bucket), 2) AS avg_next_balance_bucket,
        MIN(balance_bucket_avg_5_prior) AS min_balance_bucket_avg_5_prior
    FROM enriched_accounts
    GROUP BY CUBE (DATE_TRUNC('MONTH', opened_date), DATE_TRUNC('QUARTER', opened_date), DATE_TRUNC('YEAR', opened_date))
    HAVING open_max_next_balance_avg_6 >= 0
)
SELECT
    activity_month,
    quarter_start,
    year_start,
    open_max_next_balance_avg_6,
    min_balance_bucket_avg_5_prior
FROM daily
