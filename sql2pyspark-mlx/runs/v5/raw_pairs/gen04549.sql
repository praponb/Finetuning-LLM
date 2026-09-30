SELECT
    id,
    status,
    order_date,
    shipping_fee,
    shipping_fee - LAG(shipping_fee, 1) OVER (PARTITION BY status ORDER BY order_date, id) AS shipping_fee_change
FROM orders
ORDER BY 1;
