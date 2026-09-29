SELECT TO_DATE(paid_at) AS paid_day, AVG(amount) AS avg_amount, MIN(amount) AS min_amount
FROM payments
WHERE order_id IS NOT NULL
GROUP BY TO_DATE(paid_at)
ORDER BY min_amount DESC
