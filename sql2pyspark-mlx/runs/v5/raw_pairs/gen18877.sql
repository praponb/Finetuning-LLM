SELECT amount, 'transaction' AS source
FROM transactions
UNION
SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
WHERE plan IN ('standard', 'premium')
