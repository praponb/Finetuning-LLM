SELECT start_date, status
FROM subscriptions
WHERE status <> 'EXPIRED'
INTERSECT
SELECT start_date, status
FROM subscriptions
WHERE start_date > '2023-05-01';
