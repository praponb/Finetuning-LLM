SELECT
    id,
    customer_id,
    order_date,
    shipping_fee,
    SUM(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM orders
ORDER BY shipping_fee;
