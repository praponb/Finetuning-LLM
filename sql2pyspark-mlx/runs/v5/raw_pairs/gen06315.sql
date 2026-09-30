SELECT status, COUNT(amount) AS amount_count, SUM(amount) AS total_amount
FROM payments
GROUP BY status
ORDER BY total_amount DESC
