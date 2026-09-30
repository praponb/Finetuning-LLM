SELECT amount
FROM payments
WHERE amount >= 1170
UNION ALL
SELECT monthly_fee AS amount
FROM subscriptions
WHERE status = 'ACTIVE'
ORDER BY amount;
