SELECT
    status,
    customer_id,
    MAX(monthly_fee) AS max_monthly_fee,
    MAX(end_date) AS latest_end_date
FROM subscriptions
GROUP BY status, customer_id
