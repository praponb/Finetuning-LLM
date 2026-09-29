SELECT
    id,
    customer_id,
    started_at,
    country
FROM (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS row_num
    FROM sessions
) AS r
WHERE row_num <= 1
ORDER BY started_at DESC;
