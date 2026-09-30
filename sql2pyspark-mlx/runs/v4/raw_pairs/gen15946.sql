SELECT plan, id
FROM subscriptions
INTERSECT
SELECT plan, id
FROM subscriptions
WHERE start_date >= '2023-12-01';
