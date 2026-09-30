WITH ranked AS (
    SELECT
        id,
        status,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS rn
    FROM subscriptions
)
SELECT id, status, end_date
FROM ranked
WHERE rn <= 5
ORDER BY end_date
