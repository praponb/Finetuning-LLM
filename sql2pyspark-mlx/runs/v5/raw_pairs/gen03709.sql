WITH src AS (
    SELECT *, LOWER(status) status_lower
    FROM subscriptions
    WHERE monthly_fee BETWEEN 80 AND 130
),
summary AS (
    SELECT plan, status, SUM(monthly_fee) total_monthly_fee
    FROM src
    GROUP BY plan, status
)
SELECT *
FROM summary
