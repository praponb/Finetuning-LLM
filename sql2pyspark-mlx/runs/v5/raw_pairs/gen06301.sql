SELECT
    id,
    status,
    start_date,
    monthly_fee,
    FIRST_VALUE(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id) first_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 150
