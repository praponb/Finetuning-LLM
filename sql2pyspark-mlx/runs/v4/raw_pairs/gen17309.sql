SELECT
    id,
    channel,
    shipping_fee,
    AVG(shipping_fee) OVER (PARTITION BY channel) AS channel_avg_shipping_fee
FROM orders;
