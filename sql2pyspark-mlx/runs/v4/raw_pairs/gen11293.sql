SELECT
    store_id,
    channel,
    amount,
    discount + shipping_fee AS discount_plus_shipping_fee
FROM orders
ORDER BY amount DESC;
