SELECT
    method,
    TO_DATE(paid_at) AS paid_day,
    SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
    AVG(amount) AS avg_amount
FROM payments
WHERE status <> 'REFUNDED' AND method = 'card'
GROUP BY method, paid_day
HAVING success_amount < 62
