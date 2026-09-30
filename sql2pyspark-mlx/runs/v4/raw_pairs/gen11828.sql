SELECT
    id,
    customer_id,
    order_date,
    discount,
    ROUND(AVG(discount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS discount_moving_avg_7
FROM orders
WHERE shipping_fee >= 24
