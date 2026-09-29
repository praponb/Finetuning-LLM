SELECT
    id,
    amount,
    LOWER(method) AS method_lower,
    COALESCE(amount, 0) AS amount_filled
FROM payments
WHERE status NOT IN ('SUCCESS', 'REFUNDED') AND status IN ('REFUNDED', 'SUCCESS', 'FAILED') AND status NOT IN ('FAILED', 'REFUNDED', 'SUCCESS')
ORDER BY amount DESC, id
LIMIT 10
