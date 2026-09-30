SELECT
    id,
    channel,
    order_date,
    shipping_fee,
    store_id,
    SUM(shipping_fee) OVER (PARTITION BY channel ORDER BY order_date, id) AS running_shipping_fee,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY order_date DESC) AS order_date_pct_rank
FROM orders
ORDER BY 1 DESC;
