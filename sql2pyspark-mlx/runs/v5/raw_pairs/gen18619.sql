SELECT
    id,
    channel,
    order_date,
    shipping_fee,
    ROUND(AVG(shipping_fee) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS shipping_fee_moving_avg_3
FROM orders
WHERE store_id IS NULL;
