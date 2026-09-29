WITH ranked AS (
    SELECT
        id,
        channel,
        shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS row_num
    FROM orders
)
SELECT id, channel, shipping_fee
FROM ranked
WHERE row_num <= 1;
