SELECT *
FROM (
    SELECT
        id,
        customer_id,
        discount,
        amount,
        shipping_fee
    FROM orders
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (discount AS DISCOUNT_VAL, amount AS AMOUNT_VAL, shipping_fee AS SHIPPING_FEE_VAL)
)
