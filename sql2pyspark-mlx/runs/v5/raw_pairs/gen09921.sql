WITH top_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS pos
    FROM payments
)
SELECT id, order_id, paid_at
FROM top_payments
WHERE pos <= 3
