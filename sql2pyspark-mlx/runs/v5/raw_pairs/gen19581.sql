SELECT
    id,
    customer_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    LEAD(shipping_fee) OVER (PARTITION BY channel, status ORDER BY order_date, id) next_shipping_fee,
    LAG(amount) OVER (PARTITION BY channel, status ORDER BY order_date, id) prev_amount,
    LEAD(amount) OVER (PARTITION BY channel, status ORDER BY order_date, id) next_amount,
    ROW_NUMBER() OVER (PARTITION BY channel, status ORDER BY order_date, id) event_seq,
    SUM(amount) OVER (PARTITION BY customer_id, channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount
FROM orders;
