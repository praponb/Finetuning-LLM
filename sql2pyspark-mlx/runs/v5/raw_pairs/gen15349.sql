SELECT
    id,
    status,
    method,
    paid_at,
    CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END AS is_promptpay
FROM payments
WHERE amount <= 1430
ORDER BY method ASC, paid_at ASC;
