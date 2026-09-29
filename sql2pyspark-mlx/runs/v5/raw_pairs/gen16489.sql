SELECT
    id,
    store_id,
    order_date,
    discount,
    ROUND(AVG(discount) OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS discount_moving_avg_3
FROM orders
