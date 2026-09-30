WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        start_date,
        monthly_fee,
        status,
        SUBSTRING(plan, 1, 3) AS plan_prefix
    FROM subscriptions
    WHERE start_date >= DATE '2023-03-05'
),
valid_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END AS is_expired
    FROM valid
    WHERE plan = 'enterprise'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(is_expired) OVER w_ordered AS running_avg_is_expired,
        MAX(is_expired) OVER w_ordered AS running_max_is_expired,
        SUM(is_expired) OVER w_ordered AS running_sum_is_expired,
        LAG(monthly_fee, 1) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS prev_monthly_fee
    FROM valid_subscriptions
    WINDOW w_ordered AS (PARTITION BY status, plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(prev_monthly_fee) OVER w_recent AS running_min_prev_monthly_fee,
        AVG(prev_monthly_fee) OVER w_recent AS running_avg_prev_monthly_fee,
        COUNT(prev_monthly_fee) OVER w_recent AS running_count_prev_monthly_fee,
        SUM(is_expired) OVER (PARTITION BY plan, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_expired
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY status ORDER BY is_expired, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    start_date,
    monthly_fee,
    plan_prefix,
    is_expired,
    running_avg_is_expired,
    running_sum_is_expired,
    prev_monthly_fee,
    running_min_prev_monthly_fee
FROM with_windows_subscriptions
ORDER BY monthly_fee DESC, is_expired
