SELECT
    id,
    start_date,
    plan,
    end_date
FROM subscriptions
WHERE start_date < (SELECT MIN(start_date) FROM subscriptions WHERE end_date < '2025-10-01');
