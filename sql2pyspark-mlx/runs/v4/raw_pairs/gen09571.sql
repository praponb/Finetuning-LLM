WITH top_subscriptions AS (
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS row_num
    FROM subscriptions
)
SELECT
    id,
    plan,
    end_date,
    monthly_fee
FROM top_subscriptions
WHERE row_num = 1
