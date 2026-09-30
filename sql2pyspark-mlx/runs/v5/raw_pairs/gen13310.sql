SELECT country, CAST(started_at AS DATE) AS started_at_day, LOWER(browser) AS browser_lower
FROM sessions
ORDER BY country;
