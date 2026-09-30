SELECT
    id,
    status,
    start_date,
    customer_id,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY start_date) AS dense_rnk,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS dense_rnk_2
FROM subscriptions;
