SELECT
    id,
    channel,
    order_date,
    shipping_fee,
    store_id,
    LEAD(shipping_fee, 1, 0) OVER (PARTITION BY channel ORDER BY order_date, id) AS next_shipping_fee,
    SUM(shipping_fee) OVER (PARTITION BY store_id) AS store_id_sum_shipping_fee
FROM orders
WHERE channel NOT IN ('mobile', 'web', 'store');
