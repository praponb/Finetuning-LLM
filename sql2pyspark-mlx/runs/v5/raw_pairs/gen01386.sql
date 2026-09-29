SELECT
    id,
    start_date,
    monthly_fee,
    monthly_fee - LAG(monthly_fee, 2) OVER (ORDER BY start_date, id) AS monthly_fee_change
FROM subscriptions
