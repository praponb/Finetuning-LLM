SELECT country
FROM sessions
WHERE browser IN ('Safari', 'Chrome', 'Edge')
UNION ALL
SELECT country
FROM sessions
WHERE country IN ('SG', 'TH', 'JP')
ORDER BY country
