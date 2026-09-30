SELECT status, DATE_FORMAT(end_date, 'yyyy-MM') AS end_month, COUNT(*) AS subscription_count
FROM subscriptions
GROUP BY status, DATE_FORMAT(end_date, 'yyyy-MM');
