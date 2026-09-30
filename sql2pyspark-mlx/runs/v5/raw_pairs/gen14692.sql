SELECT amount AS value
FROM payments
UNION ALL
SELECT monthly_fee AS value
FROM subscriptions
ORDER BY value;
