WITH CustomerStats AS (
    SELECT customer_id, MIN(started_at) AS min_started_at
    FROM sessions
    GROUP BY customer_id
)
SELECT c.id, c.name, C.min_started_at
FROM customers c
LEFT JOIN CustomerStats C ON c.id = C.customer_id;
