SELECT plan
FROM subscriptions
INTERSECT
SELECT plan
FROM subscriptions
WHERE status NOT IN ('CANCELLED', 'PAUSED');
