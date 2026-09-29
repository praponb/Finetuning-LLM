SELECT
    id,
    customer_id,
    monthly_fee,
    MAX(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_max_monthly_fee
FROM subscriptions
ORDER BY customer_id_max_monthly_fee DESC
