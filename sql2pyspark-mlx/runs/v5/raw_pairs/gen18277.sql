SELECT
    id,
    plan,
    start_date,
    RANK() OVER (PARTITION BY plan ORDER BY start_date) AS start_date_rank
FROM subscriptions;
