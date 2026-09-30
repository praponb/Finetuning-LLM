SELECT
    status,
    COUNT(*) AS payment_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE order_id IN (9, 14, 18, 21) AND TO_DATE(paid_at) < '2024-02-16' AND status = 'REFUNDED'
GROUP BY 1
ORDER BY avg_amount DESC, status
LIMIT 10;
