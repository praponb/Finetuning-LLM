WITH enriched AS (
    SELECT
        *,
        COUNT(quantity) OVER (PARTITION BY product_id) AS product_id_count_quantity,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM inventory
)
SELECT
    id,
    store_id,
    product_id,
    product_id_count_quantity,
    running_quantity
FROM enriched
ORDER BY running_quantity ASC, id DESC
