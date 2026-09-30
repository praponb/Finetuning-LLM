SELECT
    id,
    store_id,
    quantity,
    MIN(quantity) OVER w_hist AS running_min_quantity,
    SUM(quantity) OVER w_hist AS running_sum_quantity,
    MAX(quantity) OVER w_hist AS running_max_quantity,
    SUM(quantity) OVER w_ordered AS running_quantity
FROM inventory
WHERE quantity != 127
WINDOW w_hist AS (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
