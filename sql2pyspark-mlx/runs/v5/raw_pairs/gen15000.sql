WITH cleaned AS (
    SELECT
        id,
        customer_id,
        end_date,
        status,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE start_date > DATE '2023-06-28'
      AND customer_id IS NOT NULL
)
SELECT customer_id
FROM cleaned
EXCEPT ALL
SELECT customer_id
FROM subscriptions
WHERE customer_id IN (4, 8, 30)
