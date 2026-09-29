SELECT method, MAX(paid_at) AS latest_paid_at, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count
FROM payments
WHERE amount >= 560 AND (order_id IN (3, 5, 31) OR status <> 'SUCCESS')
GROUP BY method
