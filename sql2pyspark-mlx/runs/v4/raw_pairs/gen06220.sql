WITH cleaned AS (
    SELECT
        *,
        UPPER(device) device_upper,
        CONCAT(country, ', ', device) country_device
    FROM sessions
    WHERE browser IS NOT NULL
),
with_window AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at < sub.end_date + INTERVAL '14' DAY
),
with_window_sessions AS (
    SELECT
        s.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_window s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at BETWEEN sub.start_date AND sub.end_date
),
gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id, subscription_id, subscription_id_2) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_window_sessions
),
sessions_sessions AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at, id, subscription_id, subscription_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
attributed AS (
    SELECT
        src.*,
        sub.id AS subscription_id_3,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee_2
    FROM sessions_sessions src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at < sub.end_date + INTERVAL '7' DAY
),
attributed_sessions AS (
    SELECT
        s.*,
        sub.id AS subscription_id_4,
        sub.plan AS subscription_plan_3,
        sub.monthly_fee AS subscription_monthly_fee_3
    FROM attributed s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at < sub.end_date + INTERVAL '30' DAY
),
matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id_5,
        sub.plan AS subscription_plan_4,
        sub.monthly_fee AS subscription_monthly_fee_4
    FROM attributed_sessions e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
),
totals AS (
    SELECT
        CASE WHEN subscription_id_2 >= 10 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_2_tier,
        CASE WHEN monthly_fee >= 246 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
        MAX(started_at) AS ended_at,
        ROUND(SUM(subscription_monthly_fee_3) / NULLIF(COUNT(CASE WHEN subscription_plan_3 = 'basic' THEN 1 END), 0), 2) AS subscription_monthly_fee_3_per_basic,
        SORT_ARRAY(COLLECT_SET(subscription_plan_3)) AS subscription_plan_3_list,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN subscription_id_2 >= 10 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(SUM(subscription_monthly_fee_3) / NULLIF(COUNT(CASE WHEN subscription_plan_3 = 'basic' THEN 1 END), 0), 2) DESC) AS subscription_monthly_fee_3_per_basic_dense_rank
    FROM matched
    GROUP BY 1, 2
)
SELECT
    subscription_id_2_tier,
    monthly_fee_tier,
    ended_at,
    subscription_monthly_fee_3_per_basic,
    subscription_monthly_fee_3_per_basic_dense_rank
FROM totals
ORDER BY ended_at DESC NULLS LAST
