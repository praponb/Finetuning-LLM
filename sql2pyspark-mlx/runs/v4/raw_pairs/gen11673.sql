WITH top_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS pos
    FROM payments
    WHERE order_id IS NULL
)
SELECT
    id,
    order_id,
    amount,
    status
FROM top_payments
WHERE pos <= 5;
