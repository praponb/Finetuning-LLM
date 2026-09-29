SELECT method, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
GROUP BY method
