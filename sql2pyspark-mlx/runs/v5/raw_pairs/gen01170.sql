SELECT
    id,
    discount,
    shipping_fee,
    channel
FROM orders
WHERE channel IN ('web', 'partner', 'store')
ORDER BY shipping_fee ASC;
