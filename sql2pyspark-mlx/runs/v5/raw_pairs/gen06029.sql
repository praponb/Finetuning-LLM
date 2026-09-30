SELECT
    id,
    order_date,
    channel,
    discount,
    AVG(shipping_fee) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_5,
    LEAD(discount, 1, 0) OVER (PARTITION BY status ORDER BY discount, id) AS next_discount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS row_seq,
    NTILE(4) OVER (ORDER BY discount DESC, id) AS discount_bucket,
    ROW_NUMBER() OVER (PARTITION BY channel, store_id ORDER BY shipping_fee DESC, id) AS event_seq
FROM orders
