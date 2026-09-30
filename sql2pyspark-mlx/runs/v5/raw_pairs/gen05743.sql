WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        start_date,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) monthly_fee_avg_5,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_monthly_fee
    FROM subscriptions
    WHERE status IN ('ACTIVE', 'CANCELLED')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') start_date_label,
        DATE_TRUNC('week', start_date) start_date_period
    FROM scored
    WHERE start_date >= DATE '2023-09-01'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT
    SELECT id
    FROM subscriptions
    WHERE status NOT IN ('ACTIVE', 'EXPIRED', 'PAUSED')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        last_known_monthly_fee,
        start_date_label,
        start_date_period,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_avg_5) monthly_fee_avg_5_percent_rank,
        SUM(last_known_monthly_fee) OVER (PARTITION BY customer_id) customer_id_sum_last_known_monthly_fee,
        MAX(monthly_fee_avg_5) OVER (PARTITION BY customer_id ORDER BY CAST(start_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) monthly_fee_avg_5_max_30d,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_avg_5) monthly_fee_avg_5_dense_rank
    FROM reconciled
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        last_known_monthly_fee,
        start_date_label,
        start_date_period,
        customer_id_sum_last_known_monthly_fee,
        monthly_fee_avg_5_max_30d,
        monthly_fee_avg_5_dense_rank,
        COUNT(monthly_fee_avg_5_percent_rank) OVER (ORDER BY CAST(start_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) monthly_fee_avg_5_percent_rank_count_7d
    FROM enriched
)
SELECT
    start_date_label,
    start_date_period,
    customer_id_sum_last_known_monthly_fee,
    monthly_fee_avg_5_max_30d,
    monthly_fee_avg_5_dense_rank
FROM with_windows
ORDER BY customer_id_sum_last_known_monthly_fee;
