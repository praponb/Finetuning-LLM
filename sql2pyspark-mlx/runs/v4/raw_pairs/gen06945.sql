WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        FLOOR(quantity) AS quantity_whole,
        DATE_ADD(last_restock, 45) AS last_restock_plus_45d
    FROM inventory
    WHERE store_id IN (1, 2, 3, 5)
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    quantity_whole,
    last_restock_plus_45d,
    AVG(quantity_whole) OVER (PARTITION BY product_id ORDER BY last_restock_plus_45d, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_whole_avg_5,
    LAST_VALUE(quantity_whole, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY last_restock_plus_45d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_whole,
    AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_avg_6
FROM base
