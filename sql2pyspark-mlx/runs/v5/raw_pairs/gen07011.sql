WITH PaymentsByOrder AS (
    SELECT order_id, SUM(amount) AS total_amount, COUNT(amount) AS amount_count
    FROM payments
    WHERE method IN ('bank_transfer', 'cash', 'promptpay')
    GROUP BY order_id
)
SELECT
    o.store_id,
    o.discount,
    P.total_amount,
    P.amount_count
FROM orders o
LEFT OUTER JOIN PaymentsByOrder P ON o.id = P.order_id
WHERE P.total_amount > 5700;
