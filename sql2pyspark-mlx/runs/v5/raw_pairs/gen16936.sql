SELECT method, MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY method
HAVING MAX(amount) - MIN(amount) < 73
