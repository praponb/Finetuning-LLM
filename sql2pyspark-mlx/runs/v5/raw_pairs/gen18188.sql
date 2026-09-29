SELECT
    id,
    customer_id,
    started_at,
    country
FROM (
    SELECT
        id,
        customer_id,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS row_num
    FROM sessions
) AS ranked
WHERE row_num <= 1;
