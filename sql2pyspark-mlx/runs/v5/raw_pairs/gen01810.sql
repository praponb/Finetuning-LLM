SELECT country, customer_id
FROM sessions
WHERE browser IN ('Firefox', 'Chrome')
UNION
SELECT country, customer_id
FROM sessions
WHERE started_at < '2023-07-01';
