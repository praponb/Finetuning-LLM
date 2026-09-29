SELECT
    status,
    YEAR(end_date) AS end_year,
    COUNT(*) AS total_count,
    COUNT(*) AS num_subscriptions
FROM subscriptions
GROUP BY status, YEAR(end_date)
HAVING num_subscriptions > 2
ORDER BY total_count DESC, 4 DESC
