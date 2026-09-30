SELECT
    id,
    paid_at,
    DATE_FORMAT(paid_at, 'yyyy-MM-dd') AS paid_at_label,
    LOWER(status) AS status_lower
FROM payments
WHERE status IN ('REFUNDED', 'SUCCESS')
