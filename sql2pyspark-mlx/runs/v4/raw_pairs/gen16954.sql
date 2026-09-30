SELECT
    id,
    customer_id,
    started_at,
    LEAD(started_at, 1) OVER (PARTITION BY customer_id ORDER BY started_at, id) next_started_at
FROM sessions;
