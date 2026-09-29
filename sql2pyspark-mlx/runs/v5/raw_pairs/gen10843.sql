SELECT id, customer_id, browser
FROM sessions
WHERE browser IN ('Edge', 'Firefox')
UNION
SELECT id, customer_id, browser
FROM sessions
WHERE customer_id IS NOT NULL
ORDER BY browser;
