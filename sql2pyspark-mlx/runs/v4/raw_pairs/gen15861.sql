SELECT id, browser
FROM sessions
WHERE started_at < '2024-04-18'
UNION ALL
SELECT id, browser
FROM sessions
WHERE browser = 'Firefox'
ORDER BY browser DESC
