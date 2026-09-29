SELECT
    id,
    amount,
    order_id,
    SUBSTRING(status, 1, 4) AS status_prefix
FROM payments
WHERE order_id = 11;
