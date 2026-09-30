SELECT *, COALESCE(browser, 'N/A') AS browser_filled, YEAR(started_at) AS started_at_year
FROM sessions
WHERE browser = 'Safari' AND country = 'SG';
