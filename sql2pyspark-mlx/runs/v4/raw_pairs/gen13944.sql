SELECT YEAR(paid_at) AS paid_year, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE order_id IS NOT NULL AND (status NOT IN ('SUCCESS', 'REFUNDED', 'FAILED') OR order_id IN (19, 26, 28, 32))
GROUP BY YEAR(paid_at);
