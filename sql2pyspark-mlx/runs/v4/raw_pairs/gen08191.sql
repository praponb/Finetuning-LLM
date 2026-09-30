SELECT country, device, HOUR(started_at) AS started_at_hour
FROM sessions
ORDER BY started_at_hour DESC;
