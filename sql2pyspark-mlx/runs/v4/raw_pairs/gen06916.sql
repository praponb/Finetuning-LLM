SELECT DISTINCT browser
FROM sessions
WHERE device NOT IN ('desktop', 'tablet');
