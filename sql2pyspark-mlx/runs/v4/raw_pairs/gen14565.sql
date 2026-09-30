SELECT id, end_date, start_date
FROM subscriptions
WHERE plan NOT IN ('premium', 'basic', 'enterprise') AND YEAR(end_date) = 2024;
