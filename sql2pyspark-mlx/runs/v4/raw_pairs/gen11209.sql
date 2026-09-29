WITH PaymentsByOrder AS (
    SELECT order_id, COUNT(DISTINCT amount) AS unique_statuss, COUNT(DISTINCT amount) AS unique_orders
    FROM payments
    WHERE amount <= 890
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    o.order_date,
    P.unique_statuss,
    P.unique_orders
FROM orders o
INNER JOIN PaymentsByOrder P ON o.id = P.order_id
ORDER BY order_date ASC;
