SELECT
    order_id,
    status,
    method,
    LENGTH(method) AS method_length
FROM payments
WHERE paid_at < '2023-05-01' AND amount IS NULL
