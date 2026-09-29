WITH with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity,
        MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity
    FROM inventory
    WHERE product_id IN (1, 5, 9)
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.running_count_quantity,
    s.running_max_quantity,
    s.running_avg_quantity,
    s.running_sum_quantity,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS avg_inventory_quantity
FROM with_windows s
ORDER BY store_id DESC NULLS FIRST
