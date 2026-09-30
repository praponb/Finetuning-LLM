SELECT
    id,
    order_date,
    shipping_fee,
    SUM(shipping_fee) OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM orders;
