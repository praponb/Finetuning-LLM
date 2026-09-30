SELECT started_at, device, SUBSTRING(country, 1, 4) AS country_prefix
FROM sessions
ORDER BY started_at DESC;
