SELECT
    channel,
    MONTH(order_date) AS order_month,
    COUNT(discount) AS discount_count,
    MAX(shipping_fee) AS max_shipping_fee,
    SUM(amount) AS total_amount
FROM orders
WHERE shipping_fee BETWEEN 23 AND 24 AND order_date > '2024-04-01'
GROUP BY channel, MONTH(order_date)
