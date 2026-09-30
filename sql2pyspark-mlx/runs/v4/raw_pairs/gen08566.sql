WITH filtered_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE monthly_fee <= 210 AND plan NOT IN ('basic', 'standard') AND status = 'EXPIRED'
),
agg AS (
    SELECT
        plan,
        DATE_TRUNC('MONTH', start_date) start_month,
        MIN(start_date) first_start_date,
        SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) premium_monthly_fee
    FROM filtered_subscriptions
    GROUP BY plan, DATE_TRUNC('MONTH', start_date)
)
SELECT plan, first_start_date, premium_monthly_fee
FROM agg
