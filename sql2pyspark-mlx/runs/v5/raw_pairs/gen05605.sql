WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-06-13')
)
SELECT plan, customer_id
FROM cleaned
EXCEPT DISTINCT
SELECT plan, customer_id
FROM subscriptions
WHERE plan NOT IN ('basic', 'enterprise')
