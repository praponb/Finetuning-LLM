SELECT
    id,
    customer_id,
    ADD_MONTHS(order_date, 6) order_date_plus_months,
    CONCAT_WS('-', channel, status) channel_status_key
FROM orders
