SELECT browser, country
FROM sessions
WHERE customer_id IN (3, 8)
UNION
SELECT browser, country
FROM sessions
WHERE browser != 'Firefox'
ORDER BY browser DESC;
