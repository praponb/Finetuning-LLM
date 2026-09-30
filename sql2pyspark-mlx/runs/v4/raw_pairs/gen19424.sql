WITH Numbered AS (
    SELECT
        id,
        customer_id,
        monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS row_num
    FROM subscriptions
    WHERE plan = 'standard'
)
SELECT id, customer_id, monthly_fee
FROM Numbered
WHERE row_num <= 5
