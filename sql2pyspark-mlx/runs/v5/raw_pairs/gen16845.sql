WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        LAG(monthly_fee, 2) OVER (ORDER BY start_date, id) AS prev_monthly_fee
    FROM subscriptions
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(monthly_fee, prev_monthly_fee) AS greatest_monthly_fee_prev_monthly_fee,
        prev_monthly_fee + first_known_monthly_fee AS prev_monthly_fee_plus_first_known_monthly_fee
    FROM scored
    WHERE prev_monthly_fee BETWEEN 170 AND 180
      AND start_date > DATE '2023-09-05'
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        first_known_monthly_fee,
        prev_monthly_fee,
        greatest_monthly_fee_prev_monthly_fee,
        prev_monthly_fee_plus_first_known_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS row_seq,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY first_known_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee
    FROM filtered
    WHERE start_date > '2025-05-01'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        first_known_monthly_fee,
        prev_monthly_fee,
        greatest_monthly_fee_prev_monthly_fee,
        prev_monthly_fee_plus_first_known_monthly_fee,
        row_seq,
        last_known_monthly_fee,
        ROUND(prev_monthly_fee * 100, 2) AS prev_monthly_fee_scaled,
        DATE_TRUNC('week', start_date) AS start_date_period
    FROM scored_subscriptions
    WHERE YEAR(start_date) = 2025
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        monthly_fee,
        prev_monthly_fee,
        greatest_monthly_fee_prev_monthly_fee,
        row_seq,
        last_known_monthly_fee,
        prev_monthly_fee_scaled,
        start_date_period,
        RANK() OVER (ORDER BY prev_monthly_fee_scaled DESC) AS prev_monthly_fee_scaled_rank,
        AVG(row_seq) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_seq_avg_6,
        AVG(prev_monthly_fee) OVER (ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_monthly_fee_avg_3,
        FIRST_VALUE(greatest_monthly_fee_prev_monthly_fee) IGNORE NULLS OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_greatest_monthly_fee_prev_monthly_fee
    FROM base
),
scored_subscriptions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(prev_monthly_fee_avg_3) OVER (ORDER BY prev_monthly_fee, id) AS prev_prev_monthly_fee_avg_3,
        LEAD(row_seq) OVER (ORDER BY prev_monthly_fee, id) AS next_row_seq,
        LAG(row_seq) OVER (ORDER BY prev_monthly_fee, id) AS prev_row_seq,
        ROW_NUMBER() OVER (ORDER BY prev_monthly_fee, id) AS row_seq_2,
        AVG(last_known_monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_avg_3
    FROM enriched
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq_avg_6 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_avg_6_tier,
    PERCENTILE_APPROX(prev_monthly_fee_scaled_rank, 0.95) AS prev_monthly_fee_scaled_rank_p95,
    ROUND(AVG(prev_monthly_fee_scaled), 2) AS avg_prev_monthly_fee_scaled,
    GROUPING_ID() AS grouping_id
FROM scored_subscriptions_2
GROUP BY ROLLUP (CASE WHEN row_seq_avg_6 >= 8 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY prev_monthly_fee_scaled_rank_p95 DESC NULLS FIRST
