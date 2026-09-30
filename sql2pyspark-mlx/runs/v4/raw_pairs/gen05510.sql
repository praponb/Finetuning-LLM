SELECT
    id,
    amount,
    order_id,
    status,
    CAST(paid_at AS DATE) AS paid_at_day
FROM payments
WHERE NOT ((HOUR(paid_at) BETWEEN 11 AND 17 AND method IN ('paypal', 'cash')) OR HOUR(paid_at) BETWEEN 8 AND 12)
ORDER BY id DESC, status ASC
