WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(amount, shipping_fee) AS greatest_amount_shipping_fee
    FROM orders
    WHERE store_id IS NOT NULL
      AND channel IS DISTINCT FROM 'partner'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        shipping_fee,
        greatest_amount_shipping_fee
    FROM valid
) src
UNPIVOT (
    metric_value FOR measure IN (shipping_fee AS SHIPPING_FEE, greatest_amount_shipping_fee)
);
