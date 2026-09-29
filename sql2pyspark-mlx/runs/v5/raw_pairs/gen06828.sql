WITH order_summary AS (
    SELECT order_id, MIN(paid_at) min_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.order_date, os.min_paid_at
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
ORDER BY id DESC, order_date
LIMIT 10
