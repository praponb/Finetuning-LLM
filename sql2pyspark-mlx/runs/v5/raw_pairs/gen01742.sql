SELECT
    id,
    customer_id,
    amount,
    MIN(amount) OVER (PARTITION BY status ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
    COUNT(amount) OVER (PARTITION BY status ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
    MAX(amount) OVER (PARTITION BY status ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
    SUM(amount) OVER (PARTITION BY status ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
    SUM(shipping_fee) OVER (PARTITION BY store_id, customer_id ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM orders
