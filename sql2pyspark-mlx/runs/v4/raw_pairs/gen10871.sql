WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        HOUR(paid_at) AS paid_at_hour
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2024-05-27 20:00:00')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        method,
        status,
        amount
    FROM cleaned
) src
PIVOT (
    SUM(amount) AS total
    FOR status IN ('SUCCESS', 'REFUNDED')
);
