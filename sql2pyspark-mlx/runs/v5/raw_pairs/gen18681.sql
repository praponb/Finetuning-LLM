SELECT MIN(amount) AS min_amount
FROM payments
WHERE order_id = 20 OR HOUR(paid_at) BETWEEN 12 AND 19
ORDER BY min_amount DESC
