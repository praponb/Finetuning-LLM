SELECT
    amount,
    shipping_fee,
    store_id,
    LAST_DAY(order_date) AS order_date_month_end
FROM orders;
