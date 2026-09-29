WITH filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        DATE_TRUNC('year', end_date) AS end_date_period
    FROM subscriptions
    WHERE customer_id IS NULL
),
with_window AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM filtered a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.end_date_period BETWEEN sub.start_date AND sub.end_date
),
with_dims AS (
    SELECT
        e.*,
        p.city,
        p.name,
        p.email
    FROM with_window e
    INNER JOIN customers p
        ON e.customer_id = p.id
    WHERE LENGTH(p.name) < 11
),
with_prev AS (
    SELECT *, LAG(end_date_period) OVER (PARTITION BY plan ORDER BY end_date_period, id, subscription_id) AS prev_end_date_period
    FROM with_dims
),
gaps AS (
    SELECT *, CASE WHEN prev_end_date_period IS NULL OR end_date_period > prev_end_date_period + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_subscriptions AS (
    SELECT
        id,
        plan,
        end_date_period,
        subscription_id,
        subscription_plan,
        monthly_fee,
        name,
        email,
        SUM(session_start_flag) OVER (PARTITION BY plan ORDER BY end_date_period, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
latest AS (
    SELECT
        id,
        subscription_id,
        subscription_plan,
        monthly_fee,
        burst_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, subscription_plan ORDER BY end_date_period DESC, id, subscription_id) AS dedup_rank
        FROM sessions_subscriptions
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    SELECT
        *,
        LEAD(burst_id, 2) OVER w_ordered AS next_burst_id,
        AVG(monthly_fee) OVER w_grp AS monthly_fee_avg_3
    FROM latest
    WINDOW w_ordered AS (PARTITION BY subscription_plan ORDER BY subscription_id, id, subscription_id), w_grp AS (PARTITION BY subscription_plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    subscription_id,
    subscription_plan,
    burst_id,
    monthly_fee_avg_3,
    CASE WHEN subscription_plan RLIKE '^(premium|enterprise)$' THEN 'GROUP_A' WHEN subscription_plan RLIKE '^(basic|standard)$' THEN 'CORE' ELSE 'GROUP_B' END AS subscription_plan_zone
FROM with_windows;
