SELECT
    status,
    DATE_FORMAT(start_date, 'yyyy-MM') AS start_month,
    MAX(start_date) AS latest_start_date,
    MIN(start_date) AS first_start_date
FROM subscriptions
WHERE monthly_fee >= 220 AND (plan = 'standard' OR status = 'PAUSED')
GROUP BY status, start_month
HAVING SUM(CASE WHEN status = 'CANCELLED' THEN monthly_fee ELSE 0 END) >= 80
ORDER BY 2, first_start_date;
