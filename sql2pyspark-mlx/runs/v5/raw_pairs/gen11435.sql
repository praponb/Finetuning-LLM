SELECT
    order_id,
    DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount,
    SUM(amount) AS total_amount
FROM payments
WHERE status <> 'REFUNDED' OR status = 'FAILED'
GROUP BY order_id, DATE_FORMAT(paid_at, 'yyyy-MM')
HAVING SUM(amount) < 3700
