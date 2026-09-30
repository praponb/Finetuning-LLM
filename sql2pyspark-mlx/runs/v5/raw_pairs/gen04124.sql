WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        DATE_SUB(order_date, 83) AS order_date_minus_83d
    FROM orders
    WHERE channel IS DISTINCT FROM 'partner'
      AND channel IS NOT NULL
)
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        amount,
        shipping_fee
    FROM cleaned
) src
UNPIVOT (
    val FOR kpi IN (amount AS AMOUNT, shipping_fee AS SHIPPING_FEE)
)
