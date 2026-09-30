SELECT
    id,
    product_id,
    quantity,
    MAX(quantity) OVER w_running AS running_max_quantity,
    COUNT(quantity) OVER w_running AS running_count_quantity,
    AVG(quantity) OVER w_running AS running_avg_quantity,
    SUM(quantity) OVER w_running AS running_sum_quantity,
    MIN(quantity) OVER (PARTITION BY product_id, order_id) AS product_id_min_quantity
FROM order_items
WHERE order_id IN (2, 11, 28, 38)
WINDOW w_running AS (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
