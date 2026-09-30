WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS rn
    FROM subscriptions
)
SELECT
    id,
    plan,
    end_date,
    customer_id
FROM ranked
WHERE rn = 1
