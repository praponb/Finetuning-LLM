WITH base AS (
    SELECT *, CONCAT_WS(',', status, method) AS status_method_key
    FROM payments
    WHERE paid_at > TIMESTAMP '2024-02-25 08:00:00'
      AND method IS NOT NULL
)
SELECT *
FROM (
    SELECT
        method,
        status,
        amount
    FROM base
) src
PIVOT (
    SUM(amount) AS total
    FOR status IN ('REFUNDED', 'SUCCESS')
);
