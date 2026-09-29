SELECT *
FROM (
    SELECT
        id,
        plan,
        end_date,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS rn
    FROM subscriptions
) AS t
WHERE rn <= 1
ORDER BY plan, id
