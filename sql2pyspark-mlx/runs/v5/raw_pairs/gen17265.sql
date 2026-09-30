WITH metrics AS (
    SELECT
        id,
        product_id,
        MIN(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
        MAX(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS seq_no
    FROM inventory
)
SELECT
    id,
    product_id,
    running_min_quantity,
    running_max_quantity,
    seq_no
FROM metrics
ORDER BY running_min_quantity NULLS LAST, running_max_quantity
