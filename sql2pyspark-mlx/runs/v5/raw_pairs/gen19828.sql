WITH metrics AS (
    SELECT
        id,
        store_id,
        channel,
        LEAD(discount) OVER (PARTITION BY store_id, channel ORDER BY shipping_fee ASC, id ASC) AS next_discount,
        LAG(discount) OVER (PARTITION BY store_id, channel ORDER BY shipping_fee ASC, id ASC) AS prev_discount,
        LEAD(shipping_fee) OVER (PARTITION BY store_id, channel ORDER BY shipping_fee ASC, id ASC) AS next_shipping_fee,
        LAG(shipping_fee) OVER (PARTITION BY store_id, channel ORDER BY shipping_fee ASC, id ASC) AS prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY store_id, channel ORDER BY shipping_fee ASC, id ASC) AS seq_no
    FROM orders
)
SELECT
    id,
    channel,
    prev_shipping_fee,
    seq_no
FROM metrics
ORDER BY prev_shipping_fee DESC, channel ASC
