SELECT country, browser
FROM sessions
WHERE browser IN ('Chrome', 'Safari') AND country != 'US';
