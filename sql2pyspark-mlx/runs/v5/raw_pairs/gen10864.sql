WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        UPPER(status) AS status_upper,
        DAYOFWEEK(paid_at) AS paid_at_dayofweek
    FROM payments
    WHERE paid_at >= TO_DATE('2023-05-29')
      AND order_id IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        method,
        paid_at_dayofweek
    FROM filtered
) src
PIVOT (
    SUM(paid_at_dayofweek) AS amount
    FOR method IN ('promptpay', 'paypal')
);
