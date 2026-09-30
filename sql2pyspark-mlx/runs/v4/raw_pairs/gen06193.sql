SELECT
    status,
    MAX(amount) - MIN(amount) amount_range,
    SUM(amount) total_amount,
    MAX(amount) max_amount
FROM payments
WHERE method = 'paypal' AND order_id IN (29, 38)
GROUP BY status
ORDER BY 1 DESC;
