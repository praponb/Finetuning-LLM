SELECT *
FROM (
    SELECT
        id,
        customer_id,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS rn
    FROM subscriptions
) AS r
WHERE rn = 1;
