SELECT plan, start_date
FROM subscriptions
WHERE status IN ('EXPIRED', 'CANCELLED')
EXCEPT
SELECT plan, start_date
FROM subscriptions
WHERE status IN ('ACTIVE', 'EXPIRED')
ORDER BY start_date;
