WITH top_subscriptions AS (
    SELECT
        id,
        status,
        end_date,
        monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS row_num
    FROM subscriptions
)
SELECT
    id,
    status,
    end_date,
    monthly_fee
FROM top_subscriptions
WHERE row_num <= 2
ORDER BY monthly_fee;
