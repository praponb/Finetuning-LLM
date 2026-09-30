WITH numbered AS (
    SELECT
        id,
        status,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS row_num
    FROM subscriptions
    WHERE status = 'CANCELLED'
)
SELECT id, status, start_date
FROM numbered
WHERE row_num = 1;
