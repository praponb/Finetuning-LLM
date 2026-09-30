SELECT
    channel,
    status,
    customer_id,
    FLOOR(discount) AS discount_whole,
    TRIM(channel) AS clean_channel
FROM orders
WHERE customer_id = 20 AND customer_id IS NULL
