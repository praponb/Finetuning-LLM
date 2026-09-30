SELECT id, browser, customer_id
FROM sessions
WHERE HOUR(started_at) BETWEEN 9 AND 17
UNION
SELECT id, browser, customer_id
FROM sessions
WHERE country = 'US'
ORDER BY id DESC;
