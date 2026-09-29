SELECT
    id,
    customer_id,
    store_id,
    channel,
    amount,
    discount,
    SUM(amount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
    COUNT(amount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
    AVG(amount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
    MAX(amount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
    AVG(amount) OVER (PARTITION BY channel) AS channel_avg_amount
FROM orders
WHERE channel = 'web'
