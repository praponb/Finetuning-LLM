WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS rnk
    FROM payments
)
SELECT id, order_id, paid_at
FROM ranked
WHERE rnk = 1
ORDER BY order_id DESC, paid_at;
