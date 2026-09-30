SELECT id, started_at, LENGTH(country) AS country_length
FROM sessions
WHERE device = 'tablet'
ORDER BY id DESC
