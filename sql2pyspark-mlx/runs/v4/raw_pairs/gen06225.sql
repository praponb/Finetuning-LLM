SELECT *, CONCAT(country, '/', browser) AS country_browser
FROM sessions
WHERE device = 'desktop'
ORDER BY started_at, id
LIMIT 1;
