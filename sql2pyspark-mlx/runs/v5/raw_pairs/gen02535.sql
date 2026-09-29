SELECT monthly_fee AS value, 'subscription' AS record_type
FROM subscriptions
UNION ALL
SELECT amount AS value, 'order' AS record_type
FROM orders
ORDER BY value DESC;
