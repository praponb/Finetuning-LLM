SELECT
    id,
    store_id,
    order_date,
    channel,
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_date_label
FROM orders
