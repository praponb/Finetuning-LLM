SELECT id, monthly_fee, customer_id
FROM subscriptions
WHERE monthly_fee > (SELECT MAX(monthly_fee) FROM subscriptions)
ORDER BY id
