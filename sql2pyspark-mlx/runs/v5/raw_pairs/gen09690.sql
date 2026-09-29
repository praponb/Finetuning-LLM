WITH ranked AS (
    SELECT
        id,
        status,
        amount,
        method,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_num
    FROM payments
    WHERE status NOT IN ('SUCCESS', 'REFUNDED')
)
SELECT
    id,
    status,
    amount,
    method
FROM ranked
WHERE row_num = 1
ORDER BY id;
