WITH PaymentsByOrder AS (
    SELECT order_id, COUNT(amount) AS amount_count
    FROM payments
    WHERE status = 'FAILED'
    GROUP BY order_id
)
SELECT o.status, P.amount_count
FROM orders o
INNER JOIN PaymentsByOrder P ON o.id = P.order_id;
