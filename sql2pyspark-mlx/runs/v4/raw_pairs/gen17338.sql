SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS pos
    FROM inventory
) AS r
WHERE pos <= 3
ORDER BY store_id ASC;
