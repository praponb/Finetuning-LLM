SELECT
    id,
    quantity,
    COUNT(quantity) OVER w_running last4_count_quantity,
    AVG(quantity) OVER w_running last4_avg_quantity,
    MIN(quantity) OVER w_running last4_min_quantity
FROM inventory
WINDOW w_running AS (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
