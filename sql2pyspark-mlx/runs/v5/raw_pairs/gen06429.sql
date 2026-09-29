SELECT amount AS value, 'payment' AS record_type
FROM payments
UNION ALL
SELECT monthly_fee AS value, 'subscription' AS record_type
FROM subscriptions
WHERE end_date >= '2024-12-01';
