SELECT
    id,
    amount,
    order_id,
    status,
    CONCAT(method, '/', status) AS method_status
FROM payments
