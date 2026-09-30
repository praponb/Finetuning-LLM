WITH ranked_subscriptions AS (
    SELECT
        id,
        status,
        start_date,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS rn
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT
    id,
    status,
    start_date,
    customer_id
FROM ranked_subscriptions
WHERE rn <= 3
ORDER BY customer_id DESC;
