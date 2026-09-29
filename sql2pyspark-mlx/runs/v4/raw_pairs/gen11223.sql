SELECT YEAR(end_date) AS end_year, COUNT(*) AS subscription_count, COUNT(*) AS num_subscriptions
FROM subscriptions
GROUP BY YEAR(end_date)
HAVING COUNT(DISTINCT monthly_fee) > 2
ORDER BY subscription_count DESC
