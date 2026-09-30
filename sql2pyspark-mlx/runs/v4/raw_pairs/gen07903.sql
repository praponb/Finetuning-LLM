WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CASE WHEN amount >= 1230 THEN 'High' WHEN amount >= 560 THEN 'Medium' ELSE 'Low' END AS amount_level,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE paid_at > DATE '2024-04-11'
      AND paid_at < '2024-03-01'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        amount
    FROM cleaned
) src
PIVOT (
    AVG(amount) AS value_sum,
    COUNT(1) AS cnt
    FOR status IN ('FAILED' AS failed, 'REFUNDED' AS refunded)
);
