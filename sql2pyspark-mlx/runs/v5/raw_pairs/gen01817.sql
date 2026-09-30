SELECT
    id,
    last_restock,
    COUNT(quantity) OVER w_part AS running_count_quantity,
    MAX(quantity) OVER w_part AS running_max_quantity,
    AVG(quantity) OVER w_part AS running_avg_quantity
FROM inventory
WHERE store_id = 2
WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
