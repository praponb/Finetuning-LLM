SELECT
    id,
    shipping_fee,
    customer_id,
    channel
FROM orders
WHERE shipping_fee >= (SELECT MAX(shipping_fee) FROM orders WHERE order_date > '2025-08-01')
ORDER BY 3 DESC;
