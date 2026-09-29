SELECT
    id,
    channel,
    shipping_fee,
    store_id
FROM (
    SELECT
        id,
        channel,
        shipping_fee,
        store_id,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS row_num
    FROM orders
) AS r
WHERE row_num <= 5
