SELECT
    id,
    customer_id,
    start_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS start_date_dense_rank
FROM subscriptions
ORDER BY start_date_dense_rank DESC
