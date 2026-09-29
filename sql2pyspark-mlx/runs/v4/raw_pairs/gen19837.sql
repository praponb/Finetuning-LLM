SELECT *
FROM (
    SELECT
        id,
        channel,
        shipping_fee,
        amount,
        discount
    FROM orders
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (shipping_fee AS SHIPPING_FEE_VAL, amount AS AMOUNT_VAL, discount)
);
