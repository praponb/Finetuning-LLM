SELECT
    id,
    order_date,
    shipping_fee,
    LEAD(shipping_fee, 2) OVER (ORDER BY order_date, id) next_shipping_fee
FROM orders
ORDER BY id
