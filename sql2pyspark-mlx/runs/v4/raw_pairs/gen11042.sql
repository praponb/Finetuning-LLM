SELECT
    status,
    DATE_TRUNC('MONTH', start_date) AS start_month,
    COUNT(DISTINCT monthly_fee) AS unique_statuss,
    SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee ELSE 0 END) AS active_monthly_fee
FROM subscriptions
WHERE end_date > '2023-05-25'
GROUP BY status, start_month
HAVING COUNT(DISTINCT monthly_fee) > 2
ORDER BY unique_statuss
