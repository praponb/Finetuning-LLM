SELECT *
FROM (
    SELECT
        id,
        customer_id,
        discount,
        shipping_fee,
        amount
    FROM orders
) src
UNPIVOT (
    reading FOR metric_name IN (discount, shipping_fee, amount AS AMOUNT_VAL)
)
