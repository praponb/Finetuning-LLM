WITH ranked_subscriptions AS (
    SELECT
        id,
        status,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS rn
    FROM subscriptions
)
SELECT id, status, start_date
FROM ranked_subscriptions
WHERE rn <= 3
ORDER BY start_date DESC;
