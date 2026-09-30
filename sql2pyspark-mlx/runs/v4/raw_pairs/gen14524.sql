SELECT customer_id
FROM sessions
WHERE HOUR(started_at) BETWEEN 6 AND 11
