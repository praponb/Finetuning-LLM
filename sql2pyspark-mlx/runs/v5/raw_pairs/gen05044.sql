SELECT QUARTER(customers.signup_date) AS signup_quarter, MAX(subscriptions.monthly_fee) AS max_monthly_fee
FROM subscriptions
JOIN customers ON customers.id = subscriptions.customer_id
WHERE subscriptions.status = 'EXPIRED'
GROUP BY QUARTER(customers.signup_date)
ORDER BY max_monthly_fee DESC, signup_quarter ASC
LIMIT 20;
