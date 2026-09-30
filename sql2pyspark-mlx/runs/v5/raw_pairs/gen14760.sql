WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        status,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_sum_monthly_fee
    FROM subscriptions
    WHERE plan = 'premium'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(customer_id_sum_monthly_fee) OVER (PARTITION BY plan, status ORDER BY customer_id_sum_monthly_fee, id) AS prev_customer_id_sum_monthly_fee,
        LEAD(customer_id_sum_monthly_fee) OVER (PARTITION BY plan, status ORDER BY customer_id_sum_monthly_fee, id) AS next_customer_id_sum_monthly_fee,
        LAG(first_known_monthly_fee) OVER (PARTITION BY plan, status ORDER BY customer_id_sum_monthly_fee, id) AS prev_first_known_monthly_fee,
        LEAD(first_known_monthly_fee) OVER (PARTITION BY plan, status ORDER BY customer_id_sum_monthly_fee, id) AS next_first_known_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY customer_id_sum_monthly_fee, id) AS event_seq,
        FIRST_VALUE(customer_id_sum_monthly_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_customer_id_sum_monthly_fee
    FROM scored
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        status,
        first_known_monthly_fee,
        prev_customer_id_sum_monthly_fee,
        next_customer_id_sum_monthly_fee,
        prev_first_known_monthly_fee,
        event_seq,
        first_known_customer_id_sum_monthly_fee,
        NTILE(5) OVER w_grp AS prev_first_known_monthly_fee_bucket,
        STDDEV_SAMP(first_known_monthly_fee) OVER w_part AS plan_stddev_samp_first_known_monthly_fee,
        FIRST_VALUE(customer_id_sum_monthly_fee, TRUE) OVER w_grp_2 AS first_known_customer_id_sum_monthly_fee_2
    FROM enriched
    WINDOW w_grp AS (ORDER BY prev_first_known_monthly_fee DESC, id), w_part AS (PARTITION BY plan), w_grp_2 AS (PARTITION BY status ORDER BY customer_id_sum_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        prev_customer_id_sum_monthly_fee,
        next_customer_id_sum_monthly_fee,
        prev_first_known_monthly_fee,
        event_seq,
        first_known_customer_id_sum_monthly_fee,
        first_known_customer_id_sum_monthly_fee_2,
        FLOOR(first_known_monthly_fee) AS first_known_monthly_fee_whole,
        GREATEST(plan_stddev_samp_first_known_monthly_fee, event_seq) AS greatest_plan_stddev_samp_first_known_monthly_fee_event_seq
    FROM with_windows
    WHERE event_seq <= 3
)
-- Grouped metrics
SELECT
    CASE WHEN first_known_monthly_fee_whole >= 69 THEN 'HIGH' ELSE 'LOW' END AS first_known_monthly_fee_whole_tier,
    plan,
    SUM(prev_customer_id_sum_monthly_fee) AS total_prev_customer_id_sum_monthly_fee,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count
FROM valid
GROUP BY CUBE (CASE WHEN first_known_monthly_fee_whole >= 69 THEN 'HIGH' ELSE 'LOW' END, plan)
