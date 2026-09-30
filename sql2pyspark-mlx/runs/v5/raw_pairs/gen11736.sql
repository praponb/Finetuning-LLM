SELECT
    id,
    plan,
    start_date,
    RANK() OVER (PARTITION BY plan ORDER BY start_date) AS start_date_rank
FROM subscriptions
WHERE start_date BETWEEN '2023-07-01' AND '2024-03-01'
