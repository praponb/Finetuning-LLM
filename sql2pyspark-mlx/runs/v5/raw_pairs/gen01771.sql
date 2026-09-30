SELECT
    id,
    method,
    SUBSTRING(method, 1, 2) AS method_prefix,
    CONCAT_WS('-', method, status) AS method_status_key
FROM payments
WHERE HOUR(paid_at) BETWEEN 11 AND 15 AND method = 'cash'
