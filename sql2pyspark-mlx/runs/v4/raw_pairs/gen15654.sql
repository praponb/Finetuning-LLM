SELECT id, browser
FROM sessions
WHERE customer_id IS NOT NULL
EXCEPT
SELECT id, browser
FROM sessions
WHERE customer_id = 16
ORDER BY id;
