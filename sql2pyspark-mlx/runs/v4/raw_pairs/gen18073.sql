WITH valid AS (
    SELECT *, LENGTH(device) AS device_length
    FROM sessions
    WHERE country = 'SG'
)
SELECT customer_id, country
FROM valid
EXCEPT ALL
SELECT customer_id, country
FROM sessions
WHERE HOUR(started_at) BETWEEN 10 AND 15;
