SELECT
    id,
    shipping_fee,
    order_date,
    amount
FROM orders
WHERE shipping_fee > (SELECT AVG(shipping_fee) FROM orders WHERE channel = 'partner')
