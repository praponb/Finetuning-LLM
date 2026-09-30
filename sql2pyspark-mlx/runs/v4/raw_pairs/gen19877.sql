SELECT browser
FROM sessions
INTERSECT ALL
SELECT browser
FROM sessions
WHERE country = 'SG'
