SELECT *
FROM (
    SELECT
        id,
        store_id,
        shipping_fee,
        amount,
        discount
    FROM orders
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (shipping_fee, amount, discount AS DISCOUNT)
);
