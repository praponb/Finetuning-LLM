WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM flattened e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.created_at >= sub.start_date
       AND e.created_at < sub.end_date + INTERVAL '14' DAY
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        k,
        subscription_id,
        plan,
        SUM(subscription_id) OVER w_ordered AS running_subscription_id,
        NTILE(4) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC, id, k, subscription_id) AS monthly_fee_bucket,
        LAST_VALUE(plan) IGNORE NULLS OVER (PARTITION BY plan ORDER BY created_at, id, k, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_plan
    FROM attributed
    WINDOW w_ordered AS (PARTITION BY k ORDER BY subscription_id, id, k, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY k, plan ORDER BY monthly_fee_bucket) AS monthly_fee_bucket_dense_rank,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY k), 4) AS subscription_id_share,
        AVG(subscription_id) OVER (PARTITION BY k ORDER BY running_subscription_id, id, k, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS subscription_id_avg_5,
        AVG(monthly_fee_bucket) OVER w_hist AS plan_avg_monthly_fee_bucket,
        LEAD(monthly_fee_bucket, 1) OVER (ORDER BY monthly_fee_bucket, id, k, subscription_id) AS next_monthly_fee_bucket
    FROM scored
    WINDOW w_hist AS (PARTITION BY plan, k)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(next_monthly_fee_bucket) OVER (PARTITION BY plan, k ORDER BY running_subscription_id, id, k, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_monthly_fee_bucket,
        COUNT(next_monthly_fee_bucket) OVER (PARTITION BY plan, k ORDER BY running_subscription_id, id, k, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_monthly_fee_bucket,
        SUM(next_monthly_fee_bucket) OVER (PARTITION BY plan, k ORDER BY running_subscription_id, id, k, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_monthly_fee_bucket,
        COUNT(subscription_id_avg_5) OVER (PARTITION BY plan ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS subscription_id_avg_5_count_2h
    FROM scored_carts
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(created_at) OVER (PARTITION BY plan ORDER BY created_at, id, k, subscription_id) AS prev_created_at
    FROM with_windows
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_created_at IS NULL OR created_at > prev_created_at + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        created_at,
        k,
        subscription_id,
        plan,
        running_subscription_id,
        monthly_fee_bucket,
        last_known_plan,
        subscription_id_share,
        plan_avg_monthly_fee_bucket,
        running_min_next_monthly_fee_bucket,
        running_sum_next_monthly_fee_bucket,
        SUM(is_new_session) OVER (PARTITION BY plan ORDER BY created_at, id, k, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_min_next_monthly_fee_bucket >= 6.9 THEN 'HIGH' ELSE 'LOW' END AS running_min_next_monthly_fee_bucket_tier,
        CASE WHEN subscription_id_share >= 0.81 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_share_tier,
        subscription_id,
        ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN k = 'weight_kg' THEN 1 END), 0), 2) AS session_no_per_weight_kg,
        COUNT_IF(k = 'device') AS device_events,
        COUNT(DISTINCT k) AS distinct_ks,
        COUNT(*) AS row_count,
        ROUND(AVG(running_min_next_monthly_fee_bucket), 2) AS avg_running_min_next_monthly_fee_bucket
    FROM islands
    GROUP BY CASE WHEN running_min_next_monthly_fee_bucket >= 6.9 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN subscription_id_share >= 0.81 THEN 'LARGE' ELSE 'SMALL' END, subscription_id
)
SELECT
    running_min_next_monthly_fee_bucket_tier,
    subscription_id_share_tier,
    subscription_id,
    session_no_per_weight_kg,
    distinct_ks
FROM totals;
