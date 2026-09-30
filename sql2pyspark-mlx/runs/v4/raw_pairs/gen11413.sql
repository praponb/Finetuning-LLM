SELECT
    id,
    method,
    status,
    paid_at,
    CONCAT_WS('-', status, method) AS status_method_key
FROM payments
WHERE method NOT IN ('promptpay', 'card', 'paypal');
