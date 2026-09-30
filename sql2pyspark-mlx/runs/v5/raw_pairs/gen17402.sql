WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(amount) AS amount_whole
    FROM payments
    WHERE order_id IS NOT NULL
      AND amount <= 640
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        amount
    FROM valid
) src
PIVOT (
    SUM(amount) AS value_sum,
    COUNT(1) AS cnt
    FOR status IN ('FAILED' AS failed, 'REFUNDED' AS refunded)
);
