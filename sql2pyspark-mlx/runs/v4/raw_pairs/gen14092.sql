WITH numbered AS (
    SELECT
        id,
        method,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS pos
    FROM payments
)
SELECT id, method, paid_at
FROM numbered
WHERE pos <= 1
ORDER BY paid_at DESC
