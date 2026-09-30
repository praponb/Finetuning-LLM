WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        paid_at,
        CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE paid_at >= DATE '2023-05-25'
      AND method IS DISTINCT FROM 'promptpay'
)
-- Aggregate with conditional metrics
SELECT
    method,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE method = 'promptpay') AS promptpay_rows,
    COUNT(DISTINCT id) AS distinct_ids_2,
    GROUPING(method) AS gid
FROM base
GROUP BY CUBE (method, DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('QUARTER', paid_at))
HAVING COUNT(DISTINCT id) < 1000
