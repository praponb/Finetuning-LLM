WITH ranked_payments AS (
    SELECT
        id,
        method,
        paid_at,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS rn
    FROM payments
)
SELECT *
FROM ranked_payments
WHERE rn <= 5
ORDER BY paid_at, id
