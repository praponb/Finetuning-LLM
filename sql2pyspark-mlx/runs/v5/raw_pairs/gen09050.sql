WITH base AS (
    SELECT
        *,
        DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label,
        CAST(paid_at AS DATE) AS paid_at_day
    FROM payments
    WHERE order_id = 30
)
SELECT order_id
FROM base
INTERSECT
SELECT order_id
FROM payments
WHERE status = 'SUCCESS';
