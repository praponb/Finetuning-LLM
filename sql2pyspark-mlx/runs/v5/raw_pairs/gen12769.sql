SELECT id, channel, UPPER(channel) channel_upper
FROM orders
WHERE channel IN ('mobile', 'store', 'partner') AND shipping_fee BETWEEN 14 AND 33
