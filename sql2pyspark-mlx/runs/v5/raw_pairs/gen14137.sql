SELECT
    id,
    start_date,
    status,
    plan
FROM subscriptions
WHERE start_date = (SELECT MAX(start_date) FROM subscriptions WHERE MONTH(start_date) IN (1, 3, 10));
