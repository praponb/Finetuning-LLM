SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
UNION
SELECT amount, 'payment' AS source
FROM payments
WHERE method = 'cash'
ORDER BY source;
