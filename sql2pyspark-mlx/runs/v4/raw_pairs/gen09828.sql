SELECT
    DATE_TRUNC('MONTH', order_date) AS order_month,
    SUM(discount) AS total_discount,
    COUNT(discount) AS discount_count,
    COUNT(*) AS num_orders
FROM orders
WHERE status IN ('COMPLETED', 'CANCELLED')
GROUP BY DATE_TRUNC('MONTH', order_date)
HAVING SUM(discount) < 0.9
