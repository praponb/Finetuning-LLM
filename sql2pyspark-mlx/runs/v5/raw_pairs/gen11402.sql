SELECT
    id,
    status,
    order_date,
    shipping_fee,
    LAST_VALUE(shipping_fee) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_shipping_fee
FROM orders
