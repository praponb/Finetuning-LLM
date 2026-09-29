SELECT *
FROM (
    SELECT
        id,
        channel,
        discount,
        shipping_fee,
        amount
    FROM orders
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric IN (discount, shipping_fee, amount)
);
