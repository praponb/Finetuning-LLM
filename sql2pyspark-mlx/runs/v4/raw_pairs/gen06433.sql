WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(amount) AS amount_whole
    FROM orders
    WHERE order_date > DATE '2024-03-08'
      AND channel IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        shipping_fee,
        amount,
        discount
    FROM valid
) src
UNPIVOT (
    reading FOR measure IN (shipping_fee, amount, discount)
);
