SELECT
    *,
    MIN(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
    AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
    MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
    FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
FROM inventory
WHERE store_id = 3;
