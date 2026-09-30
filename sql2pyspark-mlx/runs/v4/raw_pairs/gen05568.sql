SELECT
    id,
    channel,
    amount,
    store_id,
    discount + amount AS discount_plus_amount
FROM orders
WHERE (shipping_fee <= 22 AND channel = 'web') OR shipping_fee > 37
ORDER BY amount DESC, id DESC
