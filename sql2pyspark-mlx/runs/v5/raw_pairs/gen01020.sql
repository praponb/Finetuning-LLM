SELECT id, status, start_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS rn
    FROM subscriptions
    WHERE customer_id IS NULL
) AS t
WHERE rn = 1
