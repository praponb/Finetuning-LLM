SELECT
    *,
    COUNT(amount) OVER (PARTITION BY customer_id, store_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount,
    SUM(amount) OVER (PARTITION BY customer_id, store_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_amount,
    MIN(amount) OVER (PARTITION BY customer_id, store_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount,
    AVG(amount) OVER (PARTITION BY customer_id, store_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount,
    SUM(shipping_fee) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM orders;
