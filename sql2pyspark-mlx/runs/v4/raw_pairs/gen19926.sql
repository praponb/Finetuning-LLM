SELECT
    id,
    order_id,
    method,
    paid_at,
    LENGTH(method) AS method_length,
    UPPER(method) AS method_upper
FROM payments
WHERE method IN ('card', 'promptpay', 'bank_transfer') OR order_id IS NOT NULL;
