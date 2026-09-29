WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        amount * discount amount_times_discount,
        CAST(amount AS INT) amount_int
    FROM orders
    WHERE MONTH(order_date) IN (2, 11, 12)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        channel,
        amount_times_discount
    FROM cleaned
) src
PIVOT (
    AVG(amount_times_discount)
    FOR channel IN ('partner' AS partner, 'web' AS web)
);
