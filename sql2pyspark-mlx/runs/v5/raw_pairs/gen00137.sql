WITH enriched AS (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_quantity,
        MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_quantity,
        ROW_NUMBER() OVER (ORDER BY last_restock ASC, id ASC) AS event_seq
    FROM inventory
    WHERE store_id IN (1, 2, 3, 6)
)
SELECT
    id,
    product_id,
    quantity,
    last3_max_quantity,
    event_seq
FROM enriched
ORDER BY last3_max_quantity DESC
