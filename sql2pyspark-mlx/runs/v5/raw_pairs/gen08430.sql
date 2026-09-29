SELECT
    id,
    plan,
    monthly_fee,
    start_date,
    ROUND(monthly_fee, 0) AS monthly_fee_rounded
FROM subscriptions
WHERE end_date <= '2023-08-01' AND customer_id IS NOT NULL
