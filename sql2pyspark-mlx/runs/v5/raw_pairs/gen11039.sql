SELECT customer_id, device
FROM sessions
INTERSECT
SELECT customer_id, device
FROM sessions
WHERE HOUR(started_at) BETWEEN 8 AND 12;
