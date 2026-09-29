SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY order_date DESC, id) event_seq,
    SUM(shipping_fee) OVER (PARTITION BY status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_shipping_fee
FROM orders
