SELECT
    id,
    status,
    customer_id,
    start_date,
    SUBSTRING(status, 1, 2) status_prefix
FROM subscriptions
WHERE status = 'CANCELLED' OR monthly_fee <= 230
ORDER BY start_date, id DESC
