SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    LEAD(shipping_fee, 2) OVER (PARTITION BY customer_id ORDER BY amount, id) AS next_shipping_fee,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share
FROM orders;
