SELECT status, COUNT(*) AS subscription_count
FROM subscriptions
GROUP BY status;
