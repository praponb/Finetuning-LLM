WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) rnk
    FROM payments
    WHERE order_id IN (12, 18, 27)
)
SELECT
    id,
    order_id,
    paid_at,
    method
FROM ranked
WHERE rnk <= 5
ORDER BY 2 DESC;
