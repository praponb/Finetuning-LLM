SELECT
    id,
    channel,
    order_date,
    shipping_fee
FROM (
    SELECT
        id,
        channel,
        order_date,
        shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date DESC, id) AS row_num
    FROM orders
) AS ranked
WHERE row_num = 1
