WITH base AS (
    SELECT *
    FROM subscriptions
    WHERE status <> 'PAUSED' AND (plan IN ('premium', 'basic', 'standard') OR status IN ('CANCELLED', 'PAUSED'))
),
subscriptions_stats AS (
    SELECT
        plan,
        DATE_TRUNC('MONTH', end_date) end_month,
        ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee,
        AVG(monthly_fee) avg_monthly_fee
    FROM base
    GROUP BY plan, DATE_TRUNC('MONTH', end_date)
)
SELECT plan, end_month, avg_monthly_fee
FROM subscriptions_stats
WHERE stddev_monthly_fee < 30
ORDER BY end_month, avg_monthly_fee DESC, plan
LIMIT 50;
