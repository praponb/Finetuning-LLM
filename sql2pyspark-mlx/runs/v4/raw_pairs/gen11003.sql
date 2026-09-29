WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE MONTH(end_date) IN (2, 5, 8)
),
summary AS (
    SELECT status, MAX(monthly_fee) AS max_monthly_fee, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM recent_subscriptions
    GROUP BY status
)
SELECT status, monthly_fee_range
FROM summary
ORDER BY monthly_fee_range DESC, status
