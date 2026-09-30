WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE plan = 'enterprise' AND end_date >= '2025-04-01'
),
SubscriptionsStats AS (
    SELECT DATE_FORMAT(end_date, 'yyyy-MM') end_month, COUNT(monthly_fee) monthly_fee_count
    FROM src
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT *
FROM SubscriptionsStats
WHERE monthly_fee_count > 9
ORDER BY monthly_fee_count DESC
