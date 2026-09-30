WITH base AS (
    SELECT
        *,
        CONCAT(plan, ', ', status) AS plan_status,
        TRIM(status) AS clean_status
    FROM subscriptions
    WHERE start_date > DATE '2024-03-01'
      AND customer_id IS NOT NULL
      AND start_date > '2024-11-01'
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee) AS monthly_fee_dense_rank,
        DENSE_RANK() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_dense_rank_2,
        ROW_NUMBER() OVER (ORDER BY end_date DESC, id) AS event_seq
    FROM base
),
with_windows AS (
    SELECT
        *,
        ROUND(monthly_fee_dense_rank / SUM(monthly_fee_dense_rank) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_dense_rank_share,
        SUM(monthly_fee_dense_rank) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_dense_rank,
        FIRST_VALUE(monthly_fee_dense_rank, TRUE) OVER (PARTITION BY status ORDER BY monthly_fee_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_dense_rank
    FROM scored
),
with_dims AS (
    SELECT
        a.*,
        p.signup_date,
        p.age
    FROM with_windows a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
scored_subscriptions AS (
    SELECT
        *,
        RANK() OVER (ORDER BY first_known_monthly_fee_dense_rank NULLS FIRST) AS first_known_monthly_fee_dense_rank_rank,
        SUM(monthly_fee_dense_rank) OVER (PARTITION BY plan) AS plan_sum_monthly_fee_dense_rank,
        MAX(monthly_fee_dense_rank) OVER (PARTITION BY status) AS status_max_monthly_fee_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY running_monthly_fee_dense_rank, id) AS row_seq
    FROM with_dims
),
metrics AS (
    SELECT
        *,
        ROUND(first_known_monthly_fee_dense_rank_rank / SUM(first_known_monthly_fee_dense_rank_rank) OVER (PARTITION BY plan), 4) AS first_known_monthly_fee_dense_rank_rank_share,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY first_known_monthly_fee_dense_rank_rank, id) AS seq_no
    FROM scored_subscriptions
),
filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status,
        plan_status,
        clean_status,
        monthly_fee_dense_rank,
        monthly_fee_dense_rank_2,
        event_seq,
        monthly_fee_dense_rank_share,
        first_known_monthly_fee_dense_rank,
        signup_date,
        age,
        first_known_monthly_fee_dense_rank_rank,
        plan_sum_monthly_fee_dense_rank,
        row_seq,
        first_known_monthly_fee_dense_rank_rank_share,
        seq_no,
        TRIM(status) AS clean_status_2
    FROM metrics
    WHERE end_date >= TO_DATE('2023-08-14')
      AND monthly_fee_dense_rank_2 = 5
)
SELECT
    DATE_TRUNC('WEEK', end_date) AS week_start,
    customer_id,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(SUM(monthly_fee_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'standard' THEN 1 END), 0), 2) AS monthly_fee_dense_rank_per_standard,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(SUM(plan_sum_monthly_fee_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS plan_sum_monthly_fee_dense_rank_per_basic
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', end_date), customer_id), (DATE_TRUNC('WEEK', end_date)), ())
ORDER BY week_start DESC;
