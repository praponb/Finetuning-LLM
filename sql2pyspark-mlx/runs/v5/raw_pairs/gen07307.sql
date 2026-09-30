SELECT
    id,
    store_id,
    shipping_fee,
    discount,
    LOWER(channel) AS channel_lower
FROM orders
