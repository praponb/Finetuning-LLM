WITH order_totals AS (
    SELECT order_id, AVG(amount) AS avg_amount, MIN(paid_at) AS first_paid_at
    FROM payments
    WHERE paid_at < '2024-02-01'
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    o.store_id,
    ot.avg_amount,
    ot.first_paid_at
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
ORDER BY id DESC, ot.first_paid_at
