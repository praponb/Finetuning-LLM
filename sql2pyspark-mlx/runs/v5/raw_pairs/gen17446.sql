SELECT
    order_id,
    paid_at,
    ROUND(amount * 1.07, 2) AS amount_scaled,
    TO_DATE(paid_at) AS paid_at_date
FROM payments
WHERE order_id IN (2, 4, 5, 17) AND HOUR(paid_at) BETWEEN 10 AND 14 AND amount < 1040
ORDER BY amount_scaled DESC;
