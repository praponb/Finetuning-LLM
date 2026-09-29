WITH filtered AS (
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label
    FROM payments
    WHERE order_id IN (10, 28, 39)
)
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    amount_filled,
    paid_at_label,
    LAST_VALUE(amount, TRUE) OVER (PARTITION BY status, method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_seq
FROM filtered
