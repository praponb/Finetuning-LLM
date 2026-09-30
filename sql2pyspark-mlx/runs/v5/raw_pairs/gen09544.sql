SELECT amount AS value
FROM payments
WHERE status <> 'SUCCESS'
EXCEPT
SELECT monthly_fee AS value
FROM subscriptions;
