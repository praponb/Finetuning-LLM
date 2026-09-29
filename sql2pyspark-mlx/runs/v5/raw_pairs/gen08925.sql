WITH base AS (
    SELECT
        id,
        plan,
        start_date,
        monthly_fee,
        LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE end_date >= DATE '2023-06-27'
      AND customer_id IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS FIRST, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
metrics AS (
    SELECT
        id,
        plan,
        monthly_fee,
        LAG(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY latest_rank, id) AS prev_monthly_fee,
        NTILE(10) OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
    FROM ranked
),
metrics_subscriptions AS (
    SELECT
        id,
        prev_monthly_fee,
        monthly_fee_bucket,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee) AS monthly_fee_dense_rank,
        LEAD(prev_monthly_fee, 2) OVER (PARTITION BY plan ORDER BY monthly_fee_bucket, id) AS next_prev_monthly_fee,
        PERCENT_RANK() OVER (ORDER BY prev_monthly_fee DESC NULLS LAST) AS prev_monthly_fee_percent_rank
    FROM metrics
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY monthly_fee_bucket DESC, id) AS monthly_fee_bucket_bucket,
        ROW_NUMBER() OVER (ORDER BY prev_monthly_fee DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_bucket, id) AS row_seq,
        AVG(monthly_fee_dense_rank) OVER (ORDER BY monthly_fee_dense_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_dense_rank_avg_3_prior
    FROM metrics_subscriptions
)
SELECT
    id,
    prev_monthly_fee,
    monthly_fee_bucket,
    monthly_fee_dense_rank,
    prev_monthly_fee_percent_rank,
    monthly_fee_bucket_bucket,
    seq_no
FROM with_windows
ORDER BY prev_monthly_fee DESC NULLS LAST, prev_monthly_fee_percent_rank;
