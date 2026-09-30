SELECT
    plan,
    status,
    unique_customers,
    last_end_date
FROM (
    SELECT
        plan,
        status,
        COUNT(DISTINCT monthly_fee) AS unique_customers,
        MAX(end_date) AS last_end_date
    FROM subscriptions
    WHERE status = 'PAUSED'
    GROUP BY plan, status
) AS agg
WHERE unique_customers >= 6
ORDER BY unique_customers DESC, plan, status
LIMIT 10
