SELECT status, HOUR(paid_at) AS paid_hour, SUM(amount) AS total_amount
FROM payments
WHERE method = 'cash'
GROUP BY status, HOUR(paid_at)
