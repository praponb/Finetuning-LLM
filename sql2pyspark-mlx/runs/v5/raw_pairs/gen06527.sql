WITH scored AS (
    SELECT
        *,
        LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC) AS prev_quantity,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5
    FROM inventory
    WHERE MONTH(last_restock) IN (2, 3, 5)
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    prev_quantity
FROM scored;
