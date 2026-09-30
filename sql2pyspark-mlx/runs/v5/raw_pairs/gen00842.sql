SELECT device
FROM sessions
INTERSECT ALL
SELECT device
FROM sessions
WHERE country = 'US';
