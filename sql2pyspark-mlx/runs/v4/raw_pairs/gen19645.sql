WITH base AS (
    SELECT
        *,
        ROUND(amount * 0.85, 2) AS adjusted_amount,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM orders
    WHERE order_date >= DATE '2023-11-05'
)
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        shipping_fee,
        amount,
        adjusted_amount
    FROM base
) src
UNPIVOT (
    value FOR metric IN (shipping_fee AS SHIPPING_FEE, amount AS AMOUNT, adjusted_amount AS ADJUSTED_AMOUNT_VAL)
);
