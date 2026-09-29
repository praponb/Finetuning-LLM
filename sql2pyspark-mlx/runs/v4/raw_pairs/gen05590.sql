WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ROUND(quantity, 1) AS quantity_rounded,
        DATE_TRUNC('year', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock > DATE '2023-03-29'
)
SELECT *
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC NULLS FIRST, id) = 1
