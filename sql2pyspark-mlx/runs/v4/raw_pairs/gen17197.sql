WITH OrderTotals AS (
    SELECT order_id, COUNT(*) AS cnt, MIN(paid_at) AS min_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.customer_id,
    O.cnt,
    O.min_paid_at
FROM orders o
JOIN OrderTotals O ON o.id = O.order_id
WHERE O.cnt > 7
ORDER BY customer_id DESC, id
LIMIT 10
