SELECT amount, order_id, HOUR(paid_at) AS paid_at_hour
FROM payments
WHERE HOUR(paid_at) BETWEEN 6 AND 14 OR paid_at >= '2025-06-01'
ORDER BY amount DESC
