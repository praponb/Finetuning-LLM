SELECT id, country, customer_id
FROM sessions
WHERE customer_id IS NOT NULL
EXCEPT
SELECT id, country, customer_id
FROM sessions
WHERE country IN ('UK', 'SG', 'US');
