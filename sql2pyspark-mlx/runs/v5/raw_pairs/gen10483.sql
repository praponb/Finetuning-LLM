SELECT
    id,
    customer_id,
    plan,
    end_date,
    NTILE(2) OVER (PARTITION BY customer_id, plan ORDER BY end_date, id) AS bucket
FROM subscriptions
WHERE start_date BETWEEN '2023-05-01' AND '2024-06-28'
ORDER BY plan
