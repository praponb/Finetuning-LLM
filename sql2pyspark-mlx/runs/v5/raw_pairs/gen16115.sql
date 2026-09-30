SELECT
    id,
    customer_id,
    order_date,
    channel,
    store_id,
    shipping_fee,
    FIRST_VALUE(channel) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS first_channel,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS rn
FROM orders
WHERE discount > 0.15;
