WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('YEAR', paid_at) paid_at_period
    FROM payments
    WHERE method IS NOT DISTINCT FROM 'paypal'
      AND paid_at >= DATE '2023-07-30'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN method IN ('promptpay', 'bank_transfer') THEN 'Priority' ELSE 'Other' END method_group
    FROM base
    WHERE paid_at > TIMESTAMP '2023-08-30 23:00:00'
      AND method IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER w_running amount_avg_3_prior,
        PERCENT_RANK() OVER (ORDER BY amount DESC) amount_percent_rank,
        LAG(amount, 2) OVER w_grp prev_amount
    FROM valid
    WHERE order_id IS NULL
    WINDOW w_running AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY method ORDER BY paid_at_period, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        amount,
        paid_at_period,
        amount_percent_rank
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_avg_3_prior DESC NULLS FIRST, id) = 1
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        AVG(amount) OVER (PARTITION BY method) method_avg_amount,
        SUM(amount_percent_rank) OVER (ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount_percent_rank,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount_percent_rank) amount_percent_rank_dense_rank,
        AVG(amount_percent_rank) OVER (ORDER BY paid_at_period, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_percent_rank_avg_2,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY method ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_method
    FROM latest
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN amount >= 1210 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
        CASE WHEN amount_percent_rank_avg_2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_percent_rank_avg_2_tier,
        CASE WHEN cumulative_amount_percent_rank >= 4 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_amount_percent_rank_tier,
        PERCENTILE_APPROX(cumulative_amount_percent_rank, 0.9) AS cumulative_amount_percent_rank_p90,
        MIN(cumulative_amount_percent_rank) AS min_cumulative_amount_percent_rank,
        SUM(cumulative_amount_percent_rank) AS total_cumulative_amount_percent_rank,
        COUNT(*) AS cnt,
        MAX(amount_percent_rank_avg_2) AS max_amount_percent_rank_avg_2,
        RANK() OVER (PARTITION BY CASE WHEN amount >= 1210 THEN 'LARGE' ELSE 'SMALL' END ORDER BY PERCENTILE_APPROX(cumulative_amount_percent_rank, 0.9) DESC) AS cumulative_amount_percent_rank_p90_rank
    FROM with_windows_payments
    GROUP BY amount_tier, amount_percent_rank_avg_2_tier, cumulative_amount_percent_rank_tier
)
SELECT
    amount_tier,
    amount_percent_rank_avg_2_tier,
    cumulative_amount_percent_rank_tier,
    cumulative_amount_percent_rank_p90,
    min_cumulative_amount_percent_rank,
    total_cumulative_amount_percent_rank,
    max_amount_percent_rank_avg_2,
    cumulative_amount_percent_rank_p90_rank
FROM summary
ORDER BY amount_tier;
