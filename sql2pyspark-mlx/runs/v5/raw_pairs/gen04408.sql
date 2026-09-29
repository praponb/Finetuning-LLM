SELECT id, device
FROM sessions
WHERE browser = 'Chrome'
UNION
SELECT id, device
FROM sessions
WHERE browser NOT IN ('Firefox', 'Safari')
ORDER BY device DESC
