SELECT
    id,
    customer_id,
    start_date,
    NTILE(2) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS bucket
FROM subscriptions;
