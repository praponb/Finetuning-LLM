SELECT
    id,
    monthly_fee,
    customer_id,
    status
FROM subscriptions
WHERE monthly_fee > (SELECT MAX(monthly_fee) FROM subscriptions)
ORDER BY customer_id DESC, id DESC
