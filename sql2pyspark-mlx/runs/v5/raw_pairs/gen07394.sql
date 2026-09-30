SELECT
    id,
    status,
    order_date,
    shipping_fee,
    SUM(shipping_fee) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM orders
WHERE amount BETWEEN 1120 AND 1170
