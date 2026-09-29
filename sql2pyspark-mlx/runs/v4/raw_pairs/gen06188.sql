WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE plan IN ('premium', 'basic') AND end_date BETWEEN '2023-05-01' AND '2025-01-04'
),
summary AS (
    SELECT status, MAX(monthly_fee) AS max_monthly_fee, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count
    FROM recent_subscriptions
    GROUP BY status
)
SELECT *
FROM summary
WHERE active_count >= 29
ORDER BY active_count DESC, status;
