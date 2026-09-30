WITH top_payments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS pos
    FROM payments
)
SELECT
    id,
    method,
    amount,
    status
FROM top_payments
WHERE pos = 1
ORDER BY id ASC;
