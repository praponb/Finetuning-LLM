SELECT country
FROM sessions
WHERE country = 'JP'
UNION ALL
SELECT country
FROM sessions
WHERE customer_id = 39;
