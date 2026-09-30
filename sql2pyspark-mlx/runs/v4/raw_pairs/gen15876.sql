SELECT
    id,
    order_id,
    CAST(amount AS INT) AS amount_int,
    CONCAT_WS('-', method, status) AS method_status_key
FROM payments
WHERE TO_DATE(paid_at) < '2025-02-01' AND order_id = 30
ORDER BY id DESC, method_status_key
LIMIT 20;
