SELECT
    plan,
    status,
    monthly_fee,
    LENGTH(status) AS status_length
FROM subscriptions
WHERE plan IN ('premium', 'basic') AND status = 'ACTIVE'
ORDER BY 2 DESC;
