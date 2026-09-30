SELECT
    id,
    customer_id,
    start_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS start_date_row
FROM subscriptions
ORDER BY start_date_row DESC, customer_id DESC
