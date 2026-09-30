SELECT country, browser
FROM sessions
WHERE customer_id IS NOT NULL
EXCEPT
SELECT country, browser
FROM sessions
WHERE browser NOT IN ('Firefox', 'Safari')
ORDER BY country DESC;
